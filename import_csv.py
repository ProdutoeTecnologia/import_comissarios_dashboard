#!/usr/bin/env python3
"""
Importa vendas do CSV FunPlace para Supabase.

- Idempotente: usa upsert em (registro, produto) — não apaga vendas existentes.
- Mesmo CSV várias vezes ou CSV novo com status atualizado → atualiza a linha.

Uso:
  python import_csv.py caminho/para/Vendas.csv
"""

from __future__ import annotations

import argparse
import csv
import os
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from supabase import Client, create_client


def load_env_local() -> None:
    """Carrega variáveis do .env.local na raiz do repo (não sobrescreve o ambiente atual)."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(root, ".env.local")
    if not os.path.isfile(path):
        return
    with open(path, encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val


APPROVED_STATUS = "Aprovado"


def normalize_comissario_nome(value: Optional[str]) -> str:
    """Title case para exibição consistente (ex.: GUILHERME CARACAS → Guilherme Caracas)."""
    return (value or "").strip().title()


def normalize_forma_pagamento(value: Optional[str]) -> str:
    if not value:
        return ""

    v = value.strip()
    v_lower = v.lower()

    if v_lower == "mercado pago":
        return "Cartão"
    if v_lower == "mercado pago boleto":
        return "Boleto"
    if v_lower == "mercado pago pix":
        return "PIX"
    if v_lower == "pagaleve":
        return "PIX"

    return v


def parse_money(value: Optional[str]) -> float:
    if not value:
        return 0.0
    clean = value.replace("R$", "").replace(".", "").replace(",", ".").strip()
    try:
        return float(clean)
    except ValueError:
        return 0.0


def parse_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def digits_only(value: Optional[str]) -> str:
    if not value:
        return ""
    return "".join(ch for ch in value if ch.isdigit())


def normalize_email(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    normalized = value.strip().lower()
    return normalized or None


def normalize_phone_parts(ddd_raw: Optional[str], celular_raw: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    ddd_digits = digits_only(ddd_raw)
    celular_digits = digits_only(celular_raw)
    combined = digits_only(f"{ddd_raw or ''}{celular_raw or ''}")

    ddd: Optional[str] = ddd_digits[-2:] if len(ddd_digits) >= 2 else None
    celular = celular_digits or None

    # Fallback para CSVs em que o número vem completo na coluna Celular.
    if not ddd and len(combined) >= 10:
        ddd = combined[:2]
        celular = combined[2:]
    elif ddd and len(combined) > len(ddd) and not celular:
        celular = combined[len(ddd):]

    if celular:
        if len(celular) > 9:
            celular = celular[-9:]
        elif len(celular) < 8:
            celular = None

    return ddd, celular


def format_phone_br(ddd: Optional[str], celular: Optional[str]) -> Optional[str]:
    if not ddd or not celular:
        return None
    if len(celular) == 9:
        return f"({ddd}) {celular[:5]}-{celular[5:]}"
    if len(celular) == 8:
        return f"({ddd}) {celular[:4]}-{celular[4:]}"
    return None


def format_phone_e164(ddd: Optional[str], celular: Optional[str]) -> Optional[str]:
    if not ddd or not celular:
        return None
    return f"+55{ddd}{celular}"


def get_supabase_client() -> Client:
    url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
    if not url or not key:
        raise RuntimeError(
            "Defina SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY no ambiente antes de rodar."
        )
    return create_client(url, key)


def _scalar_fields_from_process_row(pr: Dict[str, object]) -> Dict[str, object]:
    """Campos não monetários: última linha do grupo sobrescreve (CSV mais recente na ordem do arquivo)."""
    return {
        "status": pr["status"],
        "forma_pagamento": pr["forma_pagamento"],
        "nome_comprador": pr["nome_comprador"],
        "email_comprador": pr["email_comprador"],
        "celular_comprador": pr["celular_comprador"],
        "ddd_celular": pr["ddd_celular"],
        "celular": pr["celular"],
        "celular_formatado": pr["celular_formatado"],
        "uf": pr["uf"],
        "cidade": pr["cidade"],
        "genero_produto": pr["genero_produto"],
        "data_compra": pr["data_compra"],
        "nascimento_comprador": pr["nascimento_comprador"],
        "comissario_nome": pr["comissario_nome"],
    }


def process_row(row: Dict[str, str]) -> Dict[str, object]:
    status = (row.get("StatusPedido") or "").strip()
    valor_final = parse_money(row.get("ValorFinal"))
    subtotal = parse_money(row.get("SubTotal"))
    descontos = parse_money(row.get("Descontos"))
    comissao = round(valor_final * 0.08, 2) if status == APPROVED_STATUS else 0.0
    produto = (row.get("Produto") or "").strip()
    forma_pagamento = normalize_forma_pagamento(row.get("FormaPagamento"))
    ddd_celular, celular = normalize_phone_parts(row.get("DDD_Celular"), row.get("Celular"))
    celular_formatado = format_phone_br(ddd_celular, celular)

    return {
        "registro": (row.get("Registro") or "").strip(),
        "comissario_nome": normalize_comissario_nome(row.get("Comissario")),
        "status": status,
        "nome_comprador": (row.get("Nome") or "").strip() or None,
        "email_comprador": normalize_email(row.get("Email")),
        "celular_comprador": (row.get("Celular") or "").strip() or None,
        "ddd_celular": ddd_celular,
        "celular": celular,
        "celular_formatado": celular_formatado,
        "uf": (row.get("UF") or "").strip() or None,
        "cidade": (row.get("Cidade") or "").strip() or None,
        "produto": produto,
        "forma_pagamento": forma_pagamento,
        "genero_produto": (row.get("Genero") or "").strip() or None,
        "data_compra": parse_date(row.get("DataCompra")),
        "nascimento_comprador": parse_date(row.get("Nascimento")),
        "subtotal": subtotal,
        "descontos": descontos,
        "valor_final": valor_final,
        "comissao": comissao,
    }


def ensure_comissario(client: Client, nome: str) -> Optional[str]:
    nome = normalize_comissario_nome(nome)
    if not nome:
        return None

    placeholder_email = (
        nome.lower().strip().replace(" ", ".").replace("@", "") + "@placeholder.local"
    )

    existing = (
        client.table("comissarios")
        .select("id")
        .eq("nome", nome)
        .limit(1)
        .execute()
    )
    if existing.data:
        return str(existing.data[0]["id"])

    inserted = (
        client.table("comissarios")
        .insert({"nome": nome, "email": placeholder_email, "ativo": True})
        .execute()
    )
    if not inserted.data:
        return None
    return str(inserted.data[0]["id"])


def ensure_pessoa(client: Client, nome: str, email: Optional[str] = None) -> Optional[str]:
    """
    Garante linha em `pessoas` para o comissário.

    Estratégia de matching (em ordem de prioridade):
    1. Lookup exato por nome (caso nominal).
    2. Lookup por email — permite vincular mesmo quando o nome no CSV diverge
       do cadastro (ex: "JOAO SILVA" vs "João da Silva").
    3. Criação de nova linha com nome + email (se fornecido).
    """
    nome = normalize_comissario_nome(nome)
    if not nome:
        return None

    # 1. Lookup por nome exato
    by_nome = (
        client.table("pessoas")
        .select("id")
        .eq("nome", nome)
        .limit(1)
        .execute()
    )
    if by_nome.data:
        return str(by_nome.data[0]["id"])

    # 2. Lookup por email (fallback para quando o nome diverge)
    email_norm = normalize_email(email)
    if email_norm:
        by_email = (
            client.table("pessoas")
            .select("id")
            .ilike("email", email_norm)
            .limit(1)
            .execute()
        )
        if by_email.data:
            return str(by_email.data[0]["id"])

    # 3. Cria nova pessoa com nome + email (se disponível)
    payload: Dict[str, object] = {"nome": nome, "tipo": None, "ativo": True}
    if email_norm:
        payload["email"] = email_norm

    inserted = (
        client.table("pessoas")
        .insert(payload)
        .execute()
    )
    if not inserted.data:
        return None
    return str(inserted.data[0]["id"])


def ensure_cliente(
    client: Client,
    nome: Optional[str],
    email: Optional[str],
    ddd_celular: Optional[str],
    celular: Optional[str],
    celular_formatado: Optional[str],
    cache_by_celular: Dict[str, str],
    cache_by_email: Dict[str, str],
) -> Optional[str]:
    email_norm = normalize_email(email)

    if celular and celular in cache_by_celular:
        return cache_by_celular[celular]
    if email_norm and email_norm in cache_by_email:
        return cache_by_email[email_norm]

    if celular:
        by_cel = client.table("clientes").select("id").eq("celular", celular).limit(1).execute()
        if by_cel.data:
            cliente_id = str(by_cel.data[0]["id"])
            cache_by_celular[celular] = cliente_id
            if email_norm:
                cache_by_email[email_norm] = cliente_id
            return cliente_id

    if email_norm:
        by_email = client.table("clientes").select("id").ilike("email", email_norm).limit(1).execute()
        if by_email.data:
            cliente_id = str(by_email.data[0]["id"])
            if celular:
                cache_by_celular[celular] = cliente_id
            cache_by_email[email_norm] = cliente_id
            return cliente_id

    payload = {
        "nome": (nome or "").strip() or None,
        "email": email_norm,
        "ddd_celular": ddd_celular,
        "celular": celular,
        "celular_formatado": celular_formatado,
        "celular_e164": format_phone_e164(ddd_celular, celular),
    }

    try:
        inserted = client.table("clientes").insert(payload).execute()
        if inserted.data:
            cliente_id = str(inserted.data[0]["id"])
            if celular:
                cache_by_celular[celular] = cliente_id
            if email_norm:
                cache_by_email[email_norm] = cliente_id
            return cliente_id
    except Exception:
        # Em corrida/duplicidade, refaz o lookup sem quebrar import.
        if celular:
            retry_by_cel = client.table("clientes").select("id").eq("celular", celular).limit(1).execute()
            if retry_by_cel.data:
                cliente_id = str(retry_by_cel.data[0]["id"])
                cache_by_celular[celular] = cliente_id
                if email_norm:
                    cache_by_email[email_norm] = cliente_id
                return cliente_id
        if email_norm:
            retry_by_email = client.table("clientes").select("id").ilike("email", email_norm).limit(1).execute()
            if retry_by_email.data:
                cliente_id = str(retry_by_email.data[0]["id"])
                if celular:
                    cache_by_celular[celular] = cliente_id
                cache_by_email[email_norm] = cliente_id
                return cliente_id

    return None


def _finalize_group(
    group: Dict[str, object],
    limite_cancelamento: datetime.date,
) -> None:
    """Aplica regra PIX pendente antiga + comissão coerente com status final."""
    status = str(group.get("status", "")).strip()
    valor_final = float(group.get("valor_final", 0.0))
    forma_pagamento = str(group.get("forma_pagamento", "")).strip()
    data_compra = group.get("data_compra")

    if forma_pagamento == "PIX" and status != APPROVED_STATUS:
        if isinstance(data_compra, str) and data_compra:
            try:
                data_compra_dt = datetime.strptime(data_compra, "%Y-%m-%d").date()
                if data_compra_dt < limite_cancelamento:
                    status = "Cancelado"
            except ValueError:
                pass

    group["status"] = status
    group["comissao"] = round(valor_final * 0.08, 2) if status == APPROVED_STATUS else 0.0


def _to_upsert_row(payload: Dict[str, object]) -> Dict[str, Any]:
    """Normaliza tipos para JSON (None em opcionais)."""
    out: Dict[str, Any] = {}
    for k, v in payload.items():
        if v is None:
            out[k] = None
        elif isinstance(v, float):
            out[k] = round(v, 2) if k in ("subtotal", "descontos", "valor_final", "comissao") else v
        else:
            out[k] = v
    return out


UPSERT_BATCH_SIZE = 300


def _upsert_batch(
    client: Client,
    batch: List[Dict[str, Any]],
    error_samples: List[str],
) -> Tuple[int, int]:
    """Retorna (written, errors)."""
    if not batch:
        return 0, 0
    try:
        client.table("vendas").upsert(batch, on_conflict="registro,produto").execute()
        return len(batch), 0
    except Exception as e:
        msg = f"batch ({len(batch)} rows): {e!s}"
        if len(error_samples) < 5:
            error_samples.append(msg)
        written = 0
        errors = 0
        for row in batch:
            try:
                client.table("vendas").upsert([row], on_conflict="registro,produto").execute()
                written += 1
            except Exception as e2:
                errors += 1
                if len(error_samples) < 12:
                    error_samples.append(f"registro={row.get('registro')} produto={row.get('produto')!r}: {e2!s}")
        return written, errors


def import_csv(path: str) -> None:
    load_env_local()
    client = get_supabase_client()

    started = datetime.now(timezone.utc)
    total_rows_read = 0
    rows_skipped_no_registro = 0
    rows_skipped_no_comissario = 0
    rows_skipped_atendimento = 0
    # ── NOVO ── vendas corporativas aproveitadas via campo "Corporativo"
    rows_corporativo_used = 0
    groups: Dict[Tuple[str, str], Dict[str, object]] = {}
    hoje = datetime.now().date()
    limite_cancelamento = hoje - timedelta(days=1)
    file_basename = os.path.basename(path)

    with open(path, "r", encoding="latin1", newline="") as file:
        reader = csv.DictReader(file, delimiter=";")
        for row in reader:
            total_rows_read += 1
            registro = (row.get("Registro") or "").strip()
            comissario_cell = (row.get("Comissario") or "").strip()
            comissario_raw = comissario_cell
            # ── NOVO ── lê campo Corporativo como fallback
            corporativo_raw = (row.get("Corporativo") or "").strip()
            produto = (row.get("Produto") or "").strip()
            attrib_from_corporativo = False

            # Coluna opcional: email do comissário (para matching por email como fallback)
            comissario_email_raw = (row.get("ComissarioEmail") or row.get("Comissario_Email") or "").strip()

            if not registro:
                rows_skipped_no_registro += 1
                continue

            # ── ALTERADO ──
            # Antes: se comissario_raw vazio → descartava; ATENDIMENTO → descartava sempre.
            # Agora: usa Corporativo quando Comissario vazio OU quando Comissario é ATENDIMENTO.
            if not comissario_raw:
                if corporativo_raw:
                    comissario_raw = corporativo_raw
                    rows_corporativo_used += 1
                    attrib_from_corporativo = True
                else:
                    rows_skipped_no_comissario += 1
                    continue
            elif comissario_raw.upper() == "ATENDIMENTO":
                if corporativo_raw:
                    comissario_raw = corporativo_raw
                    rows_corporativo_used += 1
                    attrib_from_corporativo = True
                else:
                    rows_skipped_atendimento += 1
                    continue

            status = (row.get("StatusPedido") or "").strip()
            subtotal = parse_money(row.get("SubTotal"))
            descontos = parse_money(row.get("Descontos"))
            valor_final = parse_money(row.get("ValorFinal"))

            group_key = (registro, produto)

            if group_key not in groups:
                base = process_row(row)
                # process_row lê só "Comissario" do CSV; quando o vendedor veio de Corporativo, corrige aqui.
                if attrib_from_corporativo:
                    base["comissario_nome"] = normalize_comissario_nome(corporativo_raw)
                base["subtotal"] = subtotal
                base["descontos"] = descontos
                base["valor_final"] = valor_final
                base["status"] = status
                base["corporativo"] = bool(corporativo_raw)
                # Email do comissário (coluna opcional no CSV — não vai para a tabela vendas)
                base["comissario_email"] = normalize_email(comissario_email_raw) if comissario_email_raw else None
                groups[group_key] = base
            else:
                group = groups[group_key]
                group["subtotal"] = float(group.get("subtotal", 0.0)) + subtotal
                group["descontos"] = float(group.get("descontos", 0.0)) + descontos
                group["valor_final"] = float(group.get("valor_final", 0.0)) + valor_final
                pr = process_row(row)
                if attrib_from_corporativo:
                    pr["comissario_nome"] = normalize_comissario_nome(corporativo_raw)
                group.update(_scalar_fields_from_process_row(pr))

    total_unique = len(groups)
    total_written = 0
    total_errors = 0
    error_samples: List[str] = []
    batch: List[Dict[str, Any]] = []
    clientes_by_celular: Dict[str, str] = {}
    clientes_by_email: Dict[str, str] = {}
    importado_em = datetime.now(timezone.utc).isoformat()

    for group in groups.values():
        _finalize_group(group, limite_cancelamento)
        comissario_nome = str(group.get("comissario_nome") or "")
        comissario_email = group.get("comissario_email") or None
        group["comissario_id"] = ensure_comissario(client, comissario_nome)
        group["pessoa_id"] = ensure_pessoa(client, comissario_nome, email=comissario_email)
        group["cliente_id"] = ensure_cliente(
            client=client,
            nome=group.get("nome_comprador"),
            email=group.get("email_comprador"),
            ddd_celular=group.get("ddd_celular"),
            celular=group.get("celular"),
            celular_formatado=group.get("celular_formatado"),
            cache_by_celular=clientes_by_celular,
            cache_by_email=clientes_by_email,
        )
        group["importado_em"] = importado_em

        # comissario_email e corporativo são campos internos (matching/controle apenas)
        # a tabela vendas não possui essas colunas — removidos antes do upsert
        upsert_group = {k: v for k, v in group.items() if k not in ("comissario_email", "corporativo")}
        batch.append(_to_upsert_row(upsert_group))
        if len(batch) >= UPSERT_BATCH_SIZE:
            w, err = _upsert_batch(client, batch, error_samples)
            total_written += w
            total_errors += err
            batch = []

    if batch:
        w, err = _upsert_batch(client, batch, error_samples)
        total_written += w
        total_errors += err

    finished = datetime.now(timezone.utc)

    notes_parts = [
        f"skipped_no_registro={rows_skipped_no_registro}",
        f"skipped_no_comissario={rows_skipped_no_comissario}",
        f"skipped_atendimento={rows_skipped_atendimento}",
        f"corporativo_used={rows_corporativo_used}",
    ]
    if error_samples:
        notes_parts.append("errors_sample: " + " | ".join(error_samples)[:4000])

    notes = "; ".join(notes_parts)

    print("--- Resumo do import ---")
    print(f"Arquivo: {file_basename}")
    print(f"Linhas lidas (corpo do CSV): {total_rows_read}")
    print(f"Vendas únicas (registro+produto) processadas: {total_unique}")
    print(f"Gravações bem-sucedidas (linhas enviadas ao upsert): {total_written}")
    print(f"Erros: {total_errors}")
    if rows_corporativo_used:
        print(f"Vendas corporativas importadas via campo Corporativo: {rows_corporativo_used}")
    if rows_skipped_no_registro or rows_skipped_no_comissario or rows_skipped_atendimento:
        print(
            f"(Ignoradas: sem registro={rows_skipped_no_registro}, "
            f"sem comissário={rows_skipped_no_comissario}, ATENDIMENTO={rows_skipped_atendimento})"
        )

    try:
        client.table("import_logs").insert(
            {
                "file_name": file_basename,
                "import_started_at": started.isoformat(),
                "import_finished_at": finished.isoformat(),
                "total_rows_read": total_rows_read,
                "total_unique_sales": total_unique,
                "total_written": total_written,
                "total_errors": total_errors,
                "notes": notes[:8000] if notes else None,
            }
        ).execute()
    except Exception as log_err:
        print(f"Aviso: não foi possível gravar import_logs ({log_err!s}). Aplique a migration add_import_logs.sql se necessário.")


def revalidate_next_cache(base_url: Optional[str], secret: Optional[str]) -> None:
    """
    Invalida o cache do Next.js chamando POST /api/backoffice/revalidate.
    Requer NEXT_PUBLIC_APP_URL (ou APP_URL) e REVALIDATE_SECRET no ambiente.
    Falha silenciosa — não interrompe o import.
    """
    if not base_url:
        print("Aviso: NEXT_PUBLIC_APP_URL/APP_URL não definido — cache do Next.js não foi invalidado.")
        return

    try:
        import urllib.request as urllib_req
        import urllib.error
        import json as json_mod

        url = base_url.rstrip("/") + "/api/backoffice/revalidate"
        headers = {"Content-Type": "application/json"}
        if secret:
            headers["Authorization"] = f"Bearer {secret}"

        req = urllib_req.Request(url, data=b"{}", headers=headers, method="POST")
        with urllib_req.urlopen(req, timeout=10) as resp:
            body = json_mod.loads(resp.read())
            print(f"Cache Next.js invalidado: {body.get('revalidated', [])}")
    except Exception as e:
        print(f"Aviso: falha ao invalidar cache Next.js ({e!s}). As mudanças aparecerão em até 60 s.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Importa CSV de vendas para Supabase (upsert idempotente).")
    parser.add_argument("csv_path", help="Caminho para o arquivo CSV (FunPlace)")
    args = parser.parse_args()
    try:
        import_csv(args.csv_path)

        # Invalida cache do Next.js para que novos dados apareçam imediatamente
        app_url = os.getenv("NEXT_PUBLIC_APP_URL") or os.getenv("APP_URL")
        revalidate_secret = os.getenv("REVALIDATE_SECRET")
        revalidate_next_cache(app_url, revalidate_secret)
    except Exception:
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
