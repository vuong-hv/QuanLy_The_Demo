import os
import sqlite3
from datetime import datetime

import pandas as pd

DB_FILE = os.environ.get('DB_FILE', 'data_nganhang.db')

TRANSACTION_COLUMNS = [
    'Chi tiết ngày tháng',
    'Tháng',
    'Ngày',
    'Tên Chủ POS',
    'Tên Công ty',
    'Máy POS',
    'Số Lô',
    'Số tiền',
    'Tiền phí khách',
    'Thành tiền sau phí',
    'Phí trả Cty',
    'Sau phí Cty',
    'Thực chuyển',
    'Lãi/Lỗ',
    'Biểu phí khách %',
    'Phí Cty %',
    'Ghi chú',
]

DIRECTORY_COLUMNS = [
    'Tên Chủ POS',
    'Tên Công ty',
    'Máy POS',
    'Phí Mặc Định',
    'Phí Công Ty',
]

NOTE_COLUMNS = ['Key', 'Note']
TRANSFER_COLUMNS = ['Ngày tháng', 'Tên Công ty', 'Ngân hàng nhận', 'Số tiền chuyển', 'Phí công ty']

TRANSACTION_DB_MAP = {
    'id': 'id',
    'Chi tiết ngày tháng': 'transaction_date',
    'Tháng': 'month_name',
    'Ngày': 'day_of_month',
    'Tên Chủ POS': 'owner_name',
    'Tên Công ty': 'company_name',
    'Máy POS': 'pos_machine',
    'Số Lô': 'batch_number',
    'Số tiền': 'amount',
    'Tiền phí khách': 'customer_fee_amount',
    'Thành tiền sau phí': 'amount_after_customer_fee',
    'Phí trả Cty': 'company_fee_amount',
    'Sau phí Cty': 'amount_after_company_fee',
    'Thực chuyển': 'transferred_amount',
    'Lãi/Lỗ': 'profit_loss',
    'Biểu phí khách %': 'customer_fee_percent',
    'Phí Cty %': 'company_fee_percent',
    'Ghi chú': 'note',
}

DIRECTORY_DB_MAP = {
    'Tên Chủ POS': 'owner_name',
    'Tên Công ty': 'company_name',
    'Máy POS': 'pos_machine',
    'Phí Mặc Định': 'default_fee',
    'Phí Công Ty': 'company_fee',
}

NOTE_DB_MAP = {'Key': 'note_key', 'Note': 'note_text'}
TRANSFER_DB_MAP = {
    'id': 'id',
    'Ngày tháng': 'transfer_date',
    'Tên Công ty': 'company_name',
    'Ngân hàng nhận': 'recipient_bank',
    'Số tiền chuyển': 'transfer_amount',
    'Phí công ty': 'company_fee',
}

NUMERIC_TRANSACTION_COLUMNS = [
    'Ngày',
    'Số tiền',
    'Tiền phí khách',
    'Thành tiền sau phí',
    'Phí trả Cty',
    'Sau phí Cty',
    'Thực chuyển',
    'Lãi/Lỗ',
]

NUMERIC_DIRECTORY_COLUMNS = ['Phí Mặc Định', 'Phí Công Ty']
NUMERIC_TRANSFER_COLUMNS = ['Số tiền chuyển', 'Phí công ty']


def init_database():
    conn = sqlite3.connect(DB_FILE)
    try:
        _create_tables(conn)
    finally:
        conn.close()


def get_all_sheets():
    conn = sqlite3.connect(DB_FILE)
    try:
        _create_tables(conn)
        df1 = pd.read_sql_query(
            """
            SELECT id, transaction_date, month_name, day_of_month, owner_name, company_name,
                   pos_machine, batch_number, amount, customer_fee_amount,
                   amount_after_customer_fee, company_fee_amount, amount_after_company_fee,
                   transferred_amount, profit_loss, customer_fee_percent,
                   company_fee_percent, note
            FROM transactions
            ORDER BY id
            """,
            conn,
        ).rename(columns={v: k for k, v in TRANSACTION_DB_MAP.items()})

        df_dm = pd.read_sql_query(
            """
            SELECT owner_name, company_name, pos_machine, default_fee, company_fee
            FROM directory_entries
            ORDER BY rowid
            """,
            conn,
        ).rename(columns={v: k for k, v in DIRECTORY_DB_MAP.items()})

        df_notes = pd.read_sql_query(
            """
            SELECT note_key, note_text
            FROM daily_notes
            ORDER BY note_key
            """,
            conn,
        ).rename(columns={v: k for k, v in NOTE_DB_MAP.items()})

        df_ct = pd.read_sql_query(
            """
            SELECT id, transfer_date, company_name, recipient_bank, transfer_amount, company_fee
            FROM company_transfers
            ORDER BY id
            """,
            conn,
        ).rename(columns={v: k for k, v in TRANSFER_DB_MAP.items()})
    finally:
        conn.close()

    return (
        _normalize_transactions(df1),
        _normalize_directory(df_dm),
        _normalize_notes(df_notes),
        _normalize_transfers(df_ct),
    )


def get_report_chu_context(filter_from_date='', filter_to_date='', filter_owner='Tất cả', filter_machine='Tất cả'):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        _create_tables(conn)
        hidden = _get_hidden_directory_sets(conn)
        where_sql, params = _build_transaction_filters(filter_from_date, filter_to_date, filter_owner, filter_machine)
        where_sql, params = _apply_hidden_transaction_filters(where_sql, params, hidden)

        report_rows = conn.execute(
            f"""
            SELECT
                transaction_date,
                owner_name,
                pos_machine,
                batch_number,
                SUM(profit_loss) AS profit_loss,
                SUM(amount) AS amount,
                SUM(customer_fee_amount) AS customer_fee_amount,
                SUM(amount_after_customer_fee) AS amount_after_customer_fee,
                SUM(transferred_amount) AS transferred_amount,
                MIN(id) AS idx
            FROM transactions
            {where_sql}
            GROUP BY transaction_date, owner_name, pos_machine, batch_number
            ORDER BY transaction_date DESC, CAST(batch_number AS REAL) ASC, batch_number ASC
            """,
            params,
        ).fetchall()

        note_lookup = {}
        if report_rows:
            note_keys = [
                f"CHU|{row['owner_name'] or ''}|{row['transaction_date'] or ''}|{row['pos_machine'] or ''}|{row['batch_number'] or ''}"
                for row in report_rows
            ]
            placeholders = ', '.join(['?'] * len(note_keys))
            note_rows = conn.execute(
                f"""
                SELECT note_key, note_text
                FROM daily_notes
                WHERE note_key IN ({placeholders})
                """,
                note_keys,
            ).fetchall()
            note_lookup = {row['note_key']: row['note_text'] or '' for row in note_rows}

        report_data = []
        totals = {
            'lai_lo': 0,
            'so_tien': 0,
            'phi_k': 0,
            'sau_phi_k': 0,
            'thuc_chuyen': 0,
        }
        for row in report_rows:
            row_data = {
                'Chi tiết ngày tháng': row['transaction_date'] or '',
                'Tên Chủ POS': row['owner_name'] or '',
                'Máy POS': row['pos_machine'] or '',
                'Số Lô': row['batch_number'] or '',
            }
            val_data = {
                'lai_lo': row['profit_loss'] or 0,
                'so_tien': row['amount'] or 0,
                'phi_k': row['customer_fee_amount'] or 0,
                'sau_phi_k': row['amount_after_customer_fee'] or 0,
                'thuc_chuyen': row['transferred_amount'] or 0,
            }
            key = f"CHU|{row_data['Tên Chủ POS']}|{row_data['Chi tiết ngày tháng']}|{row_data['Máy POS']}|{row_data['Số Lô']}"
            report_data.append(
                {
                    'row': row_data,
                    'val': val_data,
                    'note': note_lookup.get(key, ''),
                    'key': key,
                    'idx': int(row['idx']) if row['idx'] is not None else 0,
                    'is_total': False,
                }
            )
            totals['lai_lo'] += val_data['lai_lo']
            totals['so_tien'] += val_data['so_tien']
            totals['phi_k'] += val_data['phi_k']
            totals['sau_phi_k'] += val_data['sau_phi_k']
            totals['thuc_chuyen'] += val_data['thuc_chuyen']

        if report_data:
            report_data.append({'row': {'Chi tiết ngày tháng': 'TỔNG'}, 'val': totals, 'is_total': True})

        owner_summary = _get_owner_summary(conn, where_sql, params)
        available_where_sql, available_params = _apply_hidden_transaction_filters('', [], hidden)

        list_chu = [
            row['owner_name']
            for row in conn.execute(
                f"""
                SELECT DISTINCT owner_name
                FROM transactions
                {_extend_where_sql(available_where_sql, available_params, ["TRIM(COALESCE(owner_name, '')) <> ''"], [])[0]}
                ORDER BY owner_name
                """,
                _extend_where_sql(available_where_sql, available_params, ["TRIM(COALESCE(owner_name, '')) <> ''"], [])[1],
            ).fetchall()
        ]
        list_may = [
            row['pos_machine']
            for row in conn.execute(
                f"""
                SELECT DISTINCT pos_machine, company_name
                FROM transactions
                {_extend_where_sql(available_where_sql, available_params, ["TRIM(COALESCE(pos_machine, '')) <> ''"], [])[0]}
                ORDER BY pos_machine
                """,
                _extend_where_sql(available_where_sql, available_params, ["TRIM(COALESCE(pos_machine, '')) <> ''"], [])[1],
            ).fetchall()
        ]

        owner_map = {}
        owner_map_where_sql, owner_map_params = _extend_where_sql(
            available_where_sql,
            available_params,
            ["TRIM(COALESCE(owner_name, '')) <> ''", "TRIM(COALESCE(pos_machine, '')) <> ''"],
            [],
        )
        owner_machine_rows = conn.execute(
            f"""
            SELECT DISTINCT owner_name, company_name, pos_machine
            FROM transactions
            {owner_map_where_sql}
            ORDER BY owner_name, pos_machine
            """,
            owner_map_params,
        ).fetchall()
        for row in owner_machine_rows:
            owner_map.setdefault(row['owner_name'], []).append(row['pos_machine'])

        return {
            'report_data': report_data,
            'owner_summary': owner_summary,
            'list_chu': list_chu,
            'list_may': list_may,
            'owner_map': owner_map,
        }
    finally:
        conn.close()


def get_report_cty_context(filter_from_date='', filter_to_date='', filter_company='Tất cả', filter_machine='Tất cả'):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        _create_tables(conn)
        hidden = _get_hidden_directory_sets(conn)
        tx_where_sql, tx_params = _build_company_transaction_filters(filter_from_date, filter_to_date, filter_company, filter_machine)
        transfer_where_sql, transfer_params = _build_company_transfer_filters(filter_from_date, filter_to_date, filter_company)
        tx_where_sql, tx_params = _apply_hidden_transaction_filters(tx_where_sql, tx_params, hidden)
        transfer_where_sql, transfer_params = _apply_hidden_company_filters(transfer_where_sql, transfer_params, hidden)

        tx_rows = conn.execute(
            f"""
            SELECT
                transaction_date,
                company_name,
                SUM(amount) AS amount,
                SUM(amount_after_company_fee) AS amount_after_company_fee,
                SUM(amount_after_customer_fee) AS amount_after_customer_fee
            FROM transactions
            {tx_where_sql}
            GROUP BY transaction_date, company_name
            """,
            tx_params,
        ).fetchall()

        tx_detail_rows = conn.execute(
            f"""
            SELECT
                id,
                transaction_date,
                company_name,
                owner_name,
                pos_machine,
                batch_number,
                amount,
                company_fee_percent,
                company_fee_amount,
                amount_after_company_fee,
                amount_after_customer_fee,
                note
            FROM transactions
            {tx_where_sql}
            ORDER BY transaction_date DESC, company_name, id DESC
            """,
            tx_params,
        ).fetchall()

        transfer_rows = conn.execute(
            f"""
            SELECT
                transfer_date,
                company_name,
                SUM(transfer_amount) AS transfer_amount
            FROM company_transfers
            {transfer_where_sql}
            GROUP BY transfer_date, company_name
            """,
            transfer_params,
        ).fetchall()

        merged = {}
        for row in tx_rows:
            key = (row['transaction_date'] or '', row['company_name'] or '')
            merged[key] = {
                'date': key[0],
                'company': key[1],
                'bill': row['amount'] or 0,
                'after_fee_cty': row['amount_after_company_fee'] or 0,
                'paid_amount': 0,
                'real_profit': (row['amount_after_company_fee'] or 0) - (row['amount_after_customer_fee'] or 0),
                'transactions': [],
            }
        for row in tx_detail_rows:
            key = (row['transaction_date'] or '', row['company_name'] or '')
            merged.setdefault(
                key,
                {
                    'date': key[0],
                    'company': key[1],
                    'bill': 0,
                    'after_fee_cty': 0,
                    'paid_amount': 0,
                    'real_profit': 0,
                    'transactions': [],
                },
            )
            merged[key]['transactions'].append(
                {
                    'id': row['id'],
                    'owner': row['owner_name'] or '',
                    'machine': row['pos_machine'] or '',
                    'batch': row['batch_number'] or '',
                    'amount': row['amount'] or 0,
                    'fee_percent': row['company_fee_percent'] or '',
                    'fee_amount': row['company_fee_amount'] or 0,
                    'after_fee_cty': row['amount_after_company_fee'] or 0,
                    'after_fee_customer': row['amount_after_customer_fee'] or 0,
                    'note': row['note'] or '',
                }
            )
        for row in transfer_rows:
            key = (row['transfer_date'] or '', row['company_name'] or '')
            merged.setdefault(
                key,
                {
                    'date': key[0],
                    'company': key[1],
                    'bill': 0,
                    'after_fee_cty': 0,
                    'paid_amount': 0,
                    'real_profit': 0,
                    'transactions': [],
                },
            )
            merged[key]['paid_amount'] = row['transfer_amount'] or 0

        for item in merged.values():
            item['transactions'] = _annotate_report_transaction_batches(item['transactions'])

        report_items = sorted(merged.values(), key=lambda item: (item['date'], item['company']), reverse=True)
        report_data = []
        totals = {
            'bill': 0,
            'after_fee_cty': 0,
            'paid_amount': 0,
            'real_profit': 0,
        }

        note_lookup = {}
        if report_items:
            note_keys = [f"CTY-{item['company']}-{item['date']}" for item in report_items]
            placeholders = ', '.join(['?'] * len(note_keys))
            note_rows = conn.execute(
                f"""
                SELECT note_key, note_text
                FROM daily_notes
                WHERE note_key IN ({placeholders})
                """,
                note_keys,
            ).fetchall()
            note_lookup = {row['note_key']: row['note_text'] or '' for row in note_rows}

        for item in report_items:
            key = f"CTY-{item['company']}-{item['date']}"
            diff = item['after_fee_cty'] - item['paid_amount']
            report_data.append(
                {
                    'date': item['date'],
                    'company': item['company'],
                    'bill': item['bill'],
                    'after_fee_cty': item['after_fee_cty'],
                    'paid_amount': item['paid_amount'],
                    'diff': diff,
                    'real_profit': item['real_profit'],
                    'transactions': item['transactions'],
                    'key': key,
                    'note': note_lookup.get(key, ''),
                    'is_total': False,
                }
            )
            totals['bill'] += item['bill']
            totals['after_fee_cty'] += item['after_fee_cty']
            totals['paid_amount'] += item['paid_amount']
            totals['real_profit'] += item['real_profit']

        if report_data:
            report_data.append(
                {
                    'date': 'TỔNG',
                    'company': '',
                    'bill': totals['bill'],
                    'after_fee_cty': totals['after_fee_cty'],
                    'paid_amount': totals['paid_amount'],
                    'diff': totals['after_fee_cty'] - totals['paid_amount'],
                    'real_profit': totals['real_profit'],
                    'transactions': [],
                    'key': 'TOTAL',
                    'note': '',
                    'is_total': True,
                }
            )

        list_cty = [
            row['company_name']
            for row in conn.execute(
                """
                SELECT DISTINCT company_name
                FROM transactions
                WHERE TRIM(COALESCE(company_name, '')) <> ''
                ORDER BY company_name
                """
            ).fetchall()
            if row['company_name'] not in hidden['company']
        ]

        machine_where_sql, machine_params = _build_company_transaction_filters(
            filter_from_date,
            filter_to_date,
            filter_company,
        )
        machine_where_sql, machine_params = _apply_hidden_transaction_filters(machine_where_sql, machine_params, hidden)
        list_may = [
            row['pos_machine']
            for row in conn.execute(
                f"""
                SELECT DISTINCT pos_machine
                FROM transactions
                {machine_where_sql}
                  {"AND" if machine_where_sql else "WHERE"} TRIM(COALESCE(pos_machine, '')) <> ''
                ORDER BY pos_machine
                """,
                machine_params,
            ).fetchall()
            if row['pos_machine'] not in hidden['machine']
        ]

        return {
            'report_data': report_data,
            'list_cty': list_cty,
            'list_may': list_may,
        }
    finally:
        conn.close()


def _parse_batch_number(value):
    text = str(value or '').strip().replace(',', '')
    if not text:
        return None
    try:
        numeric_value = float(text)
    except ValueError:
        return None
    if not numeric_value.is_integer():
        return None
    return int(numeric_value)


def _annotate_report_transaction_batches(transactions):
    sorted_transactions = sorted(
        transactions,
        key=lambda item: (
            item['machine'],
            _parse_batch_number(item['batch']) is None,
            _parse_batch_number(item['batch']) if _parse_batch_number(item['batch']) is not None else str(item['batch']),
            item['id'],
        ),
    )
    previous_by_machine = {}
    for item in sorted_transactions:
        batch_number = _parse_batch_number(item['batch'])
        item['batch_status'] = ''
        item['batch_hint'] = ''
        if batch_number is None:
            continue
        machine = item['machine']
        previous_batch = previous_by_machine.get(machine)
        if previous_batch is None or batch_number == previous_batch + 1:
            item['batch_status'] = 'ok'
        else:
            item['batch_status'] = 'gap'
            item['batch_hint'] = f"Thiếu Lô {previous_batch + 1}"
        previous_by_machine[machine] = batch_number
    return sorted_transactions


def get_all_transactions_context(filter_from_date='', filter_to_date='', filter_owner='Tất cả', filter_company='Tất cả', filter_machine='Tất cả'):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        _create_tables(conn)
        hidden = _get_hidden_directory_sets(conn)
        where_sql, params = _build_transaction_filters(filter_from_date, filter_to_date, filter_owner, filter_machine)
        if filter_company != 'Tất cả':
            if where_sql:
                where_sql = f"{where_sql} AND company_name = ?"
            else:
                where_sql = "WHERE company_name = ?"
            params = [*params, filter_company]
        where_sql, params = _apply_hidden_transaction_filters(where_sql, params, hidden)

        transaction_rows = conn.execute(
            f"""
            SELECT
                id,
                transaction_date,
                month_name,
                day_of_month,
                owner_name,
                company_name,
                pos_machine,
                batch_number,
                amount,
                customer_fee_amount,
                amount_after_customer_fee,
                company_fee_amount,
                amount_after_company_fee,
                transferred_amount,
                profit_loss,
                customer_fee_percent,
                company_fee_percent,
                note
            FROM transactions
            {where_sql}
            ORDER BY id
            """,
            params,
        ).fetchall()

        transactions = [
            {
                'id': row['id'],
                'Chi tiết ngày tháng': row['transaction_date'] or '',
                'Tháng': row['month_name'] or '',
                'Ngày': row['day_of_month'] or 0,
                'Tên Chủ POS': row['owner_name'] or '',
                'Tên Công ty': row['company_name'] or '',
                'Máy POS': row['pos_machine'] or '',
                'Số Lô': row['batch_number'] or '',
                'Số tiền': row['amount'] or 0,
                'Tiền phí khách': row['customer_fee_amount'] or 0,
                'Thành tiền sau phí': row['amount_after_customer_fee'] or 0,
                'Phí trả Cty': row['company_fee_amount'] or 0,
                'Sau phí Cty': row['amount_after_company_fee'] or 0,
                'Thực chuyển': row['transferred_amount'] or 0,
                'Lãi/Lỗ': row['profit_loss'] or 0,
                'Biểu phí khách %': row['customer_fee_percent'] or '',
                'Phí Cty %': row['company_fee_percent'] or '',
                'Ghi chú': row['note'] or '',
            }
            for row in transaction_rows
        ]

        summary = {
            'count': len(transactions),
            'total_amount': sum(item['Số tiền'] for item in transactions),
            'total_customer_fee': sum(item['Tiền phí khách'] for item in transactions),
            'total_after_customer_fee': sum(item['Thành tiền sau phí'] for item in transactions),
            'total_company_fee': sum(item['Phí trả Cty'] for item in transactions),
            'total_after_company_fee': sum(item['Sau phí Cty'] for item in transactions),
            'total_transferred': sum(item['Thực chuyển'] for item in transactions),
            'total_profit_loss': sum(item['Lãi/Lỗ'] for item in transactions),
            'total_company_received': 0,
            'company_received_diff': 0,
        }

        visible_companies = sorted({item['Tên Công ty'] for item in transactions if item['Tên Công ty']})
        transfer_clauses = []
        transfer_params = []
        _append_date_range_filters(transfer_clauses, transfer_params, 'transfer_date', filter_from_date, filter_to_date)
        if visible_companies:
            transfer_clauses.append(f"company_name IN ({', '.join(['?'] * len(visible_companies))})")
            transfer_params.extend(visible_companies)
        else:
            transfer_clauses.append('1 = 0')
        _append_hidden_company_clause(transfer_clauses, transfer_params, hidden)

        transfer_where_sql = f"WHERE {' AND '.join(transfer_clauses)}" if transfer_clauses else ''
        company_received_row = conn.execute(
            f"""
            SELECT COALESCE(SUM(transfer_amount), 0) AS total_company_received
            FROM company_transfers
            {transfer_where_sql}
            """,
            transfer_params,
        ).fetchone()
        summary['total_company_received'] = (
            company_received_row['total_company_received']
            if company_received_row and company_received_row['total_company_received'] is not None
            else 0
        )
        summary['company_received_diff'] = summary['total_company_received'] - summary['total_after_customer_fee']

        company_transfer_summary_rows = conn.execute(
            f"""
            SELECT
                company_name,
                recipient_bank,
                COUNT(*) AS transfer_count,
                COALESCE(SUM(transfer_amount), 0) AS total_transfer_amount
            FROM company_transfers
            {transfer_where_sql}
            GROUP BY company_name, recipient_bank
            ORDER BY total_transfer_amount DESC, company_name, recipient_bank
            """,
            transfer_params,
        ).fetchall()
        company_transfer_summary = [
            {
                'Tên Công ty': row['company_name'] or '',
                'Ngân hàng nhận': row['recipient_bank'] or '',
                'Số lần': row['transfer_count'] or 0,
                'Tiền bank về': row['total_transfer_amount'] or 0,
            }
            for row in company_transfer_summary_rows
        ]

        owner_summary = _get_owner_summary(conn, where_sql, params)
        available_where_sql, available_params = _apply_hidden_transaction_filters('', [], hidden)

        list_chu = [
            row['owner_name']
            for row in conn.execute(
                f"""
                SELECT DISTINCT owner_name
                FROM transactions
                {_extend_where_sql(available_where_sql, available_params, ["TRIM(COALESCE(owner_name, '')) <> ''"], [])[0]}
                ORDER BY owner_name
                """,
                _extend_where_sql(available_where_sql, available_params, ["TRIM(COALESCE(owner_name, '')) <> ''"], [])[1],
            ).fetchall()
        ]
        list_cty = [
            row['company_name']
            for row in conn.execute(
                f"""
                SELECT DISTINCT company_name
                FROM transactions
                {_extend_where_sql(available_where_sql, available_params, ["TRIM(COALESCE(company_name, '')) <> ''"], [])[0]}
                ORDER BY company_name
                """,
                _extend_where_sql(available_where_sql, available_params, ["TRIM(COALESCE(company_name, '')) <> ''"], [])[1],
            ).fetchall()
        ]
        list_may = [
            row['pos_machine']
            for row in conn.execute(
                f"""
                SELECT DISTINCT pos_machine, company_name
                FROM transactions
                {_extend_where_sql(available_where_sql, available_params, ["TRIM(COALESCE(pos_machine, '')) <> ''"], [])[0]}
                ORDER BY pos_machine
                """,
                _extend_where_sql(available_where_sql, available_params, ["TRIM(COALESCE(pos_machine, '')) <> ''"], [])[1],
            ).fetchall()
        ]

        owner_map = {}
        owner_map_where_sql, owner_map_params = _extend_where_sql(
            available_where_sql,
            available_params,
            ["TRIM(COALESCE(owner_name, '')) <> ''", "TRIM(COALESCE(pos_machine, '')) <> ''"],
            [],
        )
        owner_machine_rows = conn.execute(
            f"""
            SELECT DISTINCT owner_name, company_name, pos_machine
            FROM transactions
            {owner_map_where_sql}
            ORDER BY owner_name, pos_machine
            """,
            owner_map_params,
        ).fetchall()
        for row in owner_machine_rows:
            owner_map.setdefault(row['owner_name'], []).append(row['pos_machine'])

        company_map = {}
        company_map_where_sql, company_map_params = _extend_where_sql(
            available_where_sql,
            available_params,
            ["TRIM(COALESCE(company_name, '')) <> ''", "TRIM(COALESCE(pos_machine, '')) <> ''"],
            [],
        )
        company_machine_rows = conn.execute(
            f"""
            SELECT DISTINCT company_name, pos_machine
            FROM transactions
            {company_map_where_sql}
            ORDER BY company_name, pos_machine
            """,
            company_map_params,
        ).fetchall()
        for row in company_machine_rows:
            company_map.setdefault(row['company_name'], []).append(row['pos_machine'])

        return {
            'transactions': transactions,
            'summary': summary,
            'company_transfer_summary': company_transfer_summary,
            'owner_summary': owner_summary,
            'list_chu': list_chu,
            'list_cty': list_cty,
            'list_may': list_may,
            'owner_map': owner_map,
            'company_map': company_map,
        }
    finally:
        conn.close()


def _get_owner_summary(conn, where_sql, params):
    owner_summary_rows = conn.execute(
        f"""
        SELECT
            owner_name,
            COUNT(*) AS transaction_count,
            SUM(amount) AS amount,
            SUM(profit_loss) AS profit_loss,
            SUM(amount_after_customer_fee) AS amount_after_customer_fee,
            SUM(transferred_amount) AS transferred_amount
        FROM transactions
        {where_sql}
        GROUP BY owner_name
        HAVING TRIM(COALESCE(owner_name, '')) <> ''
        ORDER BY amount DESC, owner_name
        """,
        params,
    ).fetchall()
    return [
        {
            'Tên Chủ POS': row['owner_name'] or '',
            'Tổng giao dịch': row['transaction_count'] or 0,
            'Số tiền': row['amount'] or 0,
            'Lãi/Lỗ': row['profit_loss'] or 0,
            'Thành tiền sau phí': row['amount_after_customer_fee'] or 0,
            'Thực chuyển': row['transferred_amount'] or 0,
        }
        for row in owner_summary_rows
    ]


def get_company_transfers_context(filter_from_date='', filter_to_date='', filter_company='Tất cả', filter_amount=''):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        _create_tables(conn)
        hidden = _get_hidden_directory_sets(conn)
        clauses = []
        params = []
        _append_date_range_filters(clauses, params, 'transfer_date', filter_from_date, filter_to_date)
        if filter_company != 'Tất cả':
            clauses.append('company_name = ?')
            params.append(filter_company)
        if filter_amount:
            try:
                clauses.append('transfer_amount = ?')
                params.append(float(str(filter_amount).replace(',', '')))
            except ValueError:
                pass
        _append_hidden_company_clause(clauses, params, hidden)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ''

        transfer_rows = conn.execute(
            f"""
            SELECT id, transfer_date, company_name, recipient_bank, transfer_amount, company_fee
            FROM company_transfers
            {where_sql}
            ORDER BY transfer_date DESC, id DESC
            """,
            params,
        ).fetchall()
        transfers = [
            {
                'id': row['id'],
                'Ngày tháng': row['transfer_date'] or '',
                'Tên Công ty': row['company_name'] or '',
                'Ngân hàng nhận': row['recipient_bank'] or '',
                'Số tiền chuyển': row['transfer_amount'] or 0,
                'Phí công ty': row['company_fee'] or 0,
            }
            for row in transfer_rows
        ]
        total_amt = sum(item['Số tiền chuyển'] for item in transfers)

        companies = [
            row['company_name']
            for row in conn.execute(
                """
                SELECT DISTINCT company_name
                FROM directory_entries
                WHERE TRIM(COALESCE(company_name, '')) <> ''
                ORDER BY company_name
                """
            ).fetchall()
            if row['company_name'] not in hidden['company']
        ]
        return {
            'transfers': transfers,
            'companies': companies,
            'total_amt': total_amt,
        }
    finally:
        conn.close()


def normalize_retail_payment_status(raw_status):
    return 'Trả' if str(raw_status or '').strip() == 'Trả' else 'Nợ'


def get_retail_customers_context(
    filter_from_due='',
    filter_to_due='',
    filter_status='Tất cả',
    filter_customer='',
    filter_card='',
):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        _create_tables(conn)
        clauses = []
        params = []
        _append_date_range_filters(clauses, params, 'due_date', filter_from_due, filter_to_due)
        if filter_customer:
            clauses.append('customer_name LIKE ?')
            params.append(f"%{filter_customer}%")
        if filter_card:
            clauses.append('card_number LIKE ?')
            params.append(f"%{filter_card}%")
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ''

        rows = conn.execute(
            f"""
            SELECT
                id,
                due_date,
                customer_name,
                card_number,
                bank_name,
                processing_amount,
                fee_percent,
                card_adjustment,
                current_debt,
                added_debt,
                paid_amount,
                fee,
                payment_status
            FROM retail_customers
            {where_sql}
            ORDER BY due_date ASC, id DESC
            """,
            params,
        ).fetchall()

        today = datetime.now().strftime('%Y-%m-%d')
        customers = []
        summary = {
            'total_count': 0,
            'total_fee': 0,
            'debt_count': 0,
            'paid_count': 0,
            'debt_fee': 0,
            'paid_fee': 0,
            'overdue_count': 0,
            'total_current_debt': 0,
            'total_added_debt': 0,
            'total_paid_amount': 0,
            'total_remaining_debt': 0,
        }

        for row in rows:
            legacy_status = normalize_retail_payment_status(row['payment_status'])
            legacy_fee = row['fee'] or 0
            current_debt = row['current_debt'] or 0
            added_debt = row['added_debt'] or 0
            paid_amount = row['paid_amount'] or 0
            if not current_debt and not added_debt and not paid_amount and legacy_fee:
                current_debt = legacy_fee
                if legacy_status == 'Trả':
                    paid_amount = legacy_fee
            remaining_debt = max(current_debt + added_debt - paid_amount, 0)
            status = 'Nợ' if remaining_debt > 0 else 'Trả'
            if filter_status in {'Nợ', 'Trả'} and status != filter_status:
                continue
            is_overdue = bool(row['due_date'] and row['due_date'] < today and status == 'Nợ')
            item = {
                'id': row['id'],
                'Ngày tháng': row['due_date'] or '',
                'Tên khách hàng': row['customer_name'] or '',
                'Số thẻ': row['card_number'] or '',
                'Ngân hàng': row['bank_name'] or '',
                'Số tiền nợ hiện tại': current_debt,
                'Số tiền nợ thêm': added_debt,
                'Số tiền đã trả': paid_amount,
                'Số còn lại': remaining_debt,
                'Tổng tiền nợ còn lại': remaining_debt,
                'Tổng còn nợ': remaining_debt,
                'Số tiền xử lý': current_debt + added_debt,
                'Phí bao nhiêu %': row['fee_percent'] or 0,
                'Thẻ -/+': row['card_adjustment'] or 0,
                'Thành tiền': remaining_debt,
                'Phí': remaining_debt,
                'Nợ/Trả': status,
                'is_overdue': is_overdue,
            }
            customers.append(item)

            summary['total_count'] += 1
            summary['total_fee'] += remaining_debt
            summary['total_current_debt'] += current_debt
            summary['total_added_debt'] += added_debt
            summary['total_paid_amount'] += paid_amount
            summary['total_remaining_debt'] += remaining_debt
            if status == 'Trả':
                summary['paid_count'] += 1
                summary['paid_fee'] += paid_amount
            else:
                summary['debt_count'] += 1
                summary['debt_fee'] += remaining_debt
            if is_overdue:
                summary['overdue_count'] += 1

        customer_names = [
            row['customer_name']
            for row in conn.execute(
                """
                SELECT DISTINCT customer_name
                FROM retail_customers
                WHERE TRIM(COALESCE(customer_name, '')) <> ''
                ORDER BY customer_name
                """
            ).fetchall()
        ]
        bank_names = [
            row['bank_name']
            for row in conn.execute(
                """
                SELECT DISTINCT bank_name
                FROM retail_customers
                WHERE TRIM(COALESCE(bank_name, '')) <> ''
                ORDER BY bank_name
                """
            ).fetchall()
        ]

        return {
            'customers': customers,
            'customer_names': customer_names,
            'bank_names': bank_names,
            'summary': summary,
        }
    finally:
        conn.close()


def get_directory_context():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        _create_tables(conn)
        hidden = _get_hidden_directory_sets(conn)
        rows = conn.execute(
            """
            SELECT owner_name, company_name, pos_machine, default_fee, company_fee
            FROM directory_entries
            ORDER BY rowid
            """
        ).fetchall()
        chu_pos_list = []
        companies = []
        may_pos = []
        all_chu_pos = []
        all_companies = []
        all_may_pos = []
        seen_chu = set()
        seen_companies = set()
        seen_all_chu = set()
        seen_all_companies = set()
        for row in rows:
            owner_name = row['owner_name'] or ''
            company_name = row['company_name'] or ''
            pos_machine = row['pos_machine'] or ''
            if owner_name and owner_name not in seen_all_chu:
                seen_all_chu.add(owner_name)
                all_chu_pos.append({'name': owner_name, 'hidden': owner_name in hidden['owner']})
            if company_name and company_name not in seen_all_companies:
                seen_all_companies.add(company_name)
                all_companies.append({'name': company_name, 'hidden': company_name in hidden['company']})
            if owner_name and owner_name not in hidden['owner'] and owner_name not in seen_chu:
                seen_chu.add(owner_name)
                chu_pos_list.append(owner_name)
            if company_name and company_name not in hidden['company'] and company_name not in seen_companies:
                seen_companies.add(company_name)
                companies.append(company_name)
            if pos_machine:
                machine_data = {
                    'Tên Chủ POS': owner_name,
                    'Tên Công ty': company_name,
                    'Máy POS': pos_machine,
                    'Phí Mặc Định': row['company_fee'] if row['company_fee'] is not None else (row['default_fee'] or 0),
                    'Phí Công Ty': row['company_fee'] or 0,
                    'Ẩn': pos_machine in hidden['machine'] or company_name in hidden['company'],
                }
                all_may_pos.append(machine_data)
                if not machine_data['Ẩn']:
                    may_pos.append(machine_data)
        return {
            'chu_pos_list': chu_pos_list,
            'companies': companies,
            'may_pos': may_pos,
            'all_chu_pos': all_chu_pos,
            'all_companies': all_companies,
            'all_may_pos': all_may_pos,
        }
    finally:
        conn.close()


def _summary_filter_sql(filter_date='', filter_item='', filter_text=''):
    clauses = ["TRIM(COALESCE(customer_name, '')) <> ''"]
    params = []
    if filter_date:
        clauses.append("summary_date = ?")
        params.append(filter_date)
    if filter_item:
        clauses.append("customer_name LIKE ?")
        params.append(f"%{filter_item}%")
    if filter_text:
        clauses.append("transaction_note LIKE ?")
        params.append(f"%{filter_text}%")
    return " AND ".join(clauses), params


def _empty_customer_daily_summary(all_items=None):
    return {
        'rows': [],
        'grand_total': 0,
        'recent_total': 0,
        'day_count': 0,
        'item_totals': [],
        'all_items': all_items or [],
        'update_rows': [],
    }


def get_customer_daily_summary(day_limit=7, filter_date='', filter_item='', filter_text=''):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        _create_tables(conn)
        all_item_rows = conn.execute(
            """
            SELECT DISTINCT customer_name
            FROM summary_entries
            WHERE TRIM(COALESCE(customer_name, '')) <> ''
            ORDER BY customer_name
            """
        ).fetchall()
        all_items = [row['customer_name'] or '' for row in all_item_rows]
        filter_sql, filter_params = _summary_filter_sql(filter_date, filter_item, filter_text)
        date_rows = conn.execute(
            f"""
            SELECT DISTINCT summary_date
            FROM summary_entries
            WHERE TRIM(COALESCE(summary_date, '')) <> ''
              AND {filter_sql}
            ORDER BY summary_date DESC
            LIMIT ?
            """,
            (*filter_params, day_limit),
        ).fetchall()
        dates = [row['summary_date'] for row in date_rows]
        if not dates:
            return _empty_customer_daily_summary(all_items)

        matched_item_rows = conn.execute(
            f"""
            SELECT DISTINCT customer_name
            FROM summary_entries
            WHERE {filter_sql}
            ORDER BY customer_name
            """,
            filter_params,
        ).fetchall()
        matched_items = [row['customer_name'] or '' for row in matched_item_rows]
        if not matched_items:
            return _empty_customer_daily_summary(all_items)

        item_placeholders = ','.join('?' for _ in matched_items)
        item_total_rows = conn.execute(
            f"""
            WITH item_totals AS (
                SELECT customer_name, COALESCE(SUM(amount), 0) AS total_amount
                FROM summary_entries
                WHERE {filter_sql}
                GROUP BY customer_name
            ),
            latest_notes AS (
                SELECT customer_name, transaction_note, summary_date, created_at
                FROM (
                    SELECT
                        customer_name,
                        transaction_note,
                        summary_date,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY customer_name
                            ORDER BY summary_date DESC, created_at DESC, id DESC
                        ) AS rn
                    FROM summary_entries
                    WHERE {filter_sql}
                )
                WHERE rn = 1
            )
            SELECT
                item_totals.customer_name,
                item_totals.total_amount,
                latest_notes.transaction_note AS latest_note,
                latest_notes.summary_date AS latest_note_date,
                latest_notes.created_at AS latest_note_time
            FROM item_totals
            LEFT JOIN latest_notes ON latest_notes.customer_name = item_totals.customer_name
            ORDER BY latest_notes.summary_date DESC,
                     latest_notes.created_at DESC,
                     item_totals.customer_name
            """,
            (*filter_params, *filter_params),
        ).fetchall()
        item_totals = [
            {
                'name': row['customer_name'] or '',
                'amount': row['total_amount'] or 0,
                'latest_note': row['latest_note'] or '',
                'latest_note_date': row['latest_note_date'] or '',
                'latest_note_time': row['latest_note_time'] or '',
            }
            for row in item_total_rows
        ]

        placeholders = ','.join('?' for _ in dates)
        all_balance_rows = conn.execute(
            f"""
            SELECT id, customer_name, amount
            FROM summary_entries
            WHERE {filter_sql}
            ORDER BY customer_name, summary_date, created_at, id
            """,
            filter_params,
        ).fetchall()
        balance_before_by_id = {}
        running_balance_by_item = {}
        for row in all_balance_rows:
            item_name = row['customer_name'] or ''
            balance_before_by_id[row['id']] = running_balance_by_item.get(item_name, 0)
            running_balance_by_item[item_name] = running_balance_by_item.get(item_name, 0) + (row['amount'] or 0)

        records = conn.execute(
            f"""
            SELECT id, summary_date, customer_name, transaction_note, amount, created_at
            FROM summary_entries
            WHERE summary_date IN ({placeholders})
              AND {filter_sql}
            ORDER BY summary_date DESC, created_at DESC, id DESC
            """,
            (*dates, *filter_params),
        ).fetchall()

        recent_entries_by_item = {}
        update_rows = []
        display_rows = []
        current_date = None
        daily_total = 0
        grand_total = 0
        for row in records:
            summary_date = row['summary_date'] or ''
            if current_date is not None and summary_date != current_date:
                display_rows.append(
                    {
                        'id': None,
                        'is_total': True,
                        'date': current_date,
                        'customer_name': 'Tổng cuối ngày',
                        'transaction_note': '',
                        'amount': daily_total,
                    }
                )
                daily_total = 0
            amount = row['amount'] or 0
            current_date = summary_date
            daily_total += amount
            grand_total += amount
            balance_before = balance_before_by_id.get(row['id'], 0)
            entry = {
                'id': row['id'],
                'date': summary_date,
                'created_at': row['created_at'] or '',
                'created_time': (row['created_at'] or '')[11:16],
                'transaction_note': row['transaction_note'] or '',
                'amount': amount,
                'balance_before': balance_before,
                'balance_after': balance_before + amount,
            }
            update_rows.append(
                {
                    'id': row['id'],
                    'date': summary_date,
                    'item_name': row['customer_name'] or '',
                    'balance_before': balance_before,
                    'amount': amount,
                    'balance_after': balance_before + amount,
                    'created_at': row['created_at'] or '',
                    'note': row['transaction_note'] or '',
                }
            )
            item_name = row['customer_name'] or ''
            recent_entries_by_item.setdefault(item_name, []).append(entry)
            display_rows.append(
                {
                    'id': row['id'],
                    'is_total': False,
                    'date': summary_date,
                    'customer_name': row['customer_name'] or '',
                    'transaction_note': row['transaction_note'] or '',
                    'amount': amount,
                }
            )

        for item in item_totals:
            item['recent_entries'] = recent_entries_by_item.get(item['name'], [])

        if current_date is not None:
            display_rows.append(
                {
                    'id': None,
                    'is_total': True,
                    'date': current_date,
                    'customer_name': 'Tổng cuối ngày',
                    'transaction_note': '',
                    'amount': daily_total,
                }
            )

        return {
            'rows': display_rows,
            'grand_total': sum(item['amount'] for item in item_totals),
            'recent_total': grand_total,
            'day_count': len(dates),
            'item_totals': item_totals,
            'all_items': all_items,
            'update_rows': update_rows,
        }
    finally:
        conn.close()


def add_summary_entry(summary_date, customer_name, amount, transaction_note='', created_at=None):
    conn = sqlite3.connect(DB_FILE)
    try:
        _create_tables(conn)
        conn.execute(
            """
            INSERT INTO summary_entries (summary_date, customer_name, transaction_note, amount, created_at)
            VALUES (?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
            """,
            (summary_date, customer_name, transaction_note, amount, created_at),
        )
        conn.commit()
    finally:
        conn.close()


def update_summary_entry_by_id(entry_id, summary_date, customer_name, amount, transaction_note='', created_at=None):
    conn = sqlite3.connect(DB_FILE)
    try:
        _create_tables(conn)
        conn.execute(
            """
            UPDATE summary_entries
            SET summary_date = ?,
                customer_name = ?,
                transaction_note = ?,
                amount = ?,
                created_at = COALESCE(?, created_at)
            WHERE id = ?
            """,
            (summary_date, customer_name, transaction_note, amount, created_at, entry_id),
        )
        conn.commit()
    finally:
        conn.close()


def delete_summary_entry_by_id(entry_id):
    conn = sqlite3.connect(DB_FILE)
    try:
        _create_tables(conn)
        conn.execute("DELETE FROM summary_entries WHERE id = ?", (entry_id,))
        conn.commit()
    finally:
        conn.close()


def get_transaction_by_id(transaction_id):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        _create_tables(conn)
        row = conn.execute(
            """
            SELECT
                id,
                transaction_date,
                month_name,
                day_of_month,
                owner_name,
                company_name,
                pos_machine,
                batch_number,
                amount,
                customer_fee_amount,
                amount_after_customer_fee,
                company_fee_amount,
                amount_after_company_fee,
                transferred_amount,
                profit_loss,
                customer_fee_percent,
                company_fee_percent,
                note
            FROM transactions
            WHERE id = ?
            """,
            (transaction_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            'id': row['id'],
            'Chi tiết ngày tháng': row['transaction_date'] or '',
            'Tháng': row['month_name'] or '',
            'Ngày': row['day_of_month'] or 0,
            'Tên Chủ POS': row['owner_name'] or '',
            'Tên Công ty': row['company_name'] or '',
            'Máy POS': row['pos_machine'] or '',
            'Số Lô': row['batch_number'] or '',
            'Số tiền': row['amount'] or 0,
            'Tiền phí khách': row['customer_fee_amount'] or 0,
            'Thành tiền sau phí': row['amount_after_customer_fee'] or 0,
            'Phí trả Cty': row['company_fee_amount'] or 0,
            'Sau phí Cty': row['amount_after_company_fee'] or 0,
            'Thực chuyển': row['transferred_amount'] or 0,
            'Lãi/Lỗ': row['profit_loss'] or 0,
            'Biểu phí khách %': row['customer_fee_percent'] or '',
            'Phí Cty %': row['company_fee_percent'] or '',
            'Ghi chú': row['note'] or '',
        }
    finally:
        conn.close()


def update_transaction_by_id(transaction_id, payload):
    conn = sqlite3.connect(DB_FILE)
    try:
        _create_tables(conn)
        conn.execute(
            """
            UPDATE transactions
            SET transaction_date = ?,
                month_name = ?,
                day_of_month = ?,
                owner_name = ?,
                company_name = ?,
                pos_machine = ?,
                batch_number = ?,
                amount = ?,
                customer_fee_amount = ?,
                amount_after_customer_fee = ?,
                company_fee_amount = ?,
                amount_after_company_fee = ?,
                transferred_amount = ?,
                profit_loss = ?,
                customer_fee_percent = ?,
                company_fee_percent = ?,
                note = ?
            WHERE id = ?
            """,
            (
                payload['transaction_date'],
                payload['month_name'],
                payload['day_of_month'],
                payload['owner_name'],
                payload['company_name'],
                payload['pos_machine'],
                payload['batch_number'],
                payload['amount'],
                payload['customer_fee_amount'],
                payload['amount_after_customer_fee'],
                payload['company_fee_amount'],
                payload['amount_after_company_fee'],
                payload['transferred_amount'],
                payload['profit_loss'],
                payload['customer_fee_percent'],
                payload['company_fee_percent'],
                payload['note'],
                transaction_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def delete_transaction_by_id(transaction_id):
    conn = sqlite3.connect(DB_FILE)
    try:
        _create_tables(conn)
        conn.execute("DELETE FROM transactions WHERE id = ?", (transaction_id,))
        conn.commit()
    finally:
        conn.close()


def delete_company_transfer_by_id(transfer_id):
    conn = sqlite3.connect(DB_FILE)
    try:
        _create_tables(conn)
        conn.execute("DELETE FROM company_transfers WHERE id = ?", (transfer_id,))
        conn.commit()
    finally:
        conn.close()


def add_retail_customer_entry(
    due_date,
    customer_name,
    card_number='',
    fee=0,
    payment_status='Nợ',
    bank_name='',
    fee_percent=0,
    processing_amount=0,
    card_adjustment=0,
    current_debt=0,
    added_debt=0,
    paid_amount=0,
):
    remaining_debt = max((current_debt or 0) + (added_debt or 0) - (paid_amount or 0), 0)
    if not fee:
        fee = remaining_debt
    if not processing_amount:
        processing_amount = (current_debt or 0) + (added_debt or 0)
    payment_status = 'Nợ' if fee > 0 else 'Trả'
    conn = sqlite3.connect(DB_FILE)
    try:
        _create_tables(conn)
        conn.execute(
            """
            INSERT INTO retail_customers (
                due_date,
                customer_name,
                card_number,
                bank_name,
                processing_amount,
                fee_percent,
                card_adjustment,
                current_debt,
                added_debt,
                paid_amount,
                fee,
                payment_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                due_date,
                customer_name,
                card_number,
                bank_name,
                processing_amount,
                fee_percent,
                card_adjustment,
                current_debt,
                added_debt,
                paid_amount,
                fee,
                normalize_retail_payment_status(payment_status),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def update_retail_customer_entry(
    customer_id,
    due_date,
    customer_name,
    card_number='',
    fee=0,
    payment_status='Nợ',
    bank_name='',
    fee_percent=0,
    processing_amount=0,
    card_adjustment=0,
    current_debt=0,
    added_debt=0,
    paid_amount=0,
):
    remaining_debt = max((current_debt or 0) + (added_debt or 0) - (paid_amount or 0), 0)
    if not fee:
        fee = remaining_debt
    if not processing_amount:
        processing_amount = (current_debt or 0) + (added_debt or 0)
    payment_status = 'Nợ' if fee > 0 else 'Trả'
    conn = sqlite3.connect(DB_FILE)
    try:
        _create_tables(conn)
        conn.execute(
            """
            UPDATE retail_customers
            SET due_date = ?,
                customer_name = ?,
                card_number = ?,
                bank_name = ?,
                processing_amount = ?,
                fee_percent = ?,
                card_adjustment = ?,
                current_debt = ?,
                added_debt = ?,
                paid_amount = ?,
                fee = ?,
                payment_status = ?
            WHERE id = ?
            """,
            (
                due_date,
                customer_name,
                card_number,
                bank_name,
                processing_amount,
                fee_percent,
                card_adjustment,
                current_debt,
                added_debt,
                paid_amount,
                fee,
                normalize_retail_payment_status(payment_status),
                customer_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def delete_retail_customer_by_id(customer_id):
    conn = sqlite3.connect(DB_FILE)
    try:
        _create_tables(conn)
        conn.execute("DELETE FROM retail_customers WHERE id = ?", (customer_id,))
        conn.commit()
    finally:
        conn.close()


def upsert_note(key, text):
    conn = sqlite3.connect(DB_FILE)
    try:
        _create_tables(conn)
        if text:
            conn.execute(
                """
                INSERT INTO daily_notes (note_key, note_text)
                VALUES (?, ?)
                ON CONFLICT(note_key) DO UPDATE SET note_text = excluded.note_text
                """,
                (key, text),
            )
        else:
            conn.execute("DELETE FROM daily_notes WHERE note_key = ?", (key,))
        conn.commit()
    finally:
        conn.close()


def add_transfer_to_transaction(key, amount, operation='add'):
    parts = key.split('|')
    if len(parts) != 5:
        return False
    delta = amount if operation != 'subtract' else -amount
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        _create_tables(conn)
        row = conn.execute(
            """
            SELECT id, transferred_amount, note
            FROM transactions
            WHERE owner_name = ?
              AND transaction_date = ?
              AND pos_machine = ?
              AND CAST(batch_number AS TEXT) = ?
            ORDER BY id
            LIMIT 1
            """,
            (parts[1], parts[2], parts[3], str(parts[4])),
        ).fetchone()
        if row is None:
            return False
        note_prefix = '+' if delta >= 0 else '-'
        current_note = row['note'] or ''
        appended_note = f"{current_note} [{note_prefix}{amount:,.0f} lúc {datetime.now().strftime('%H:%M')}]"
        conn.execute(
            """
            UPDATE transactions
            SET transferred_amount = COALESCE(transferred_amount, 0) + ?,
                note = ?
            WHERE id = ?
            """,
            (delta, appended_note, row['id']),
        )
        conn.commit()
        return True
    finally:
        conn.close()


def add_company_transfer_entry(transfer_date, company_name, amount, company_fee=0, recipient_bank=''):
    conn = sqlite3.connect(DB_FILE)
    try:
        _create_tables(conn)
        conn.execute(
            """
            INSERT INTO company_transfers (transfer_date, company_name, recipient_bank, transfer_amount, company_fee)
            VALUES (?, ?, ?, ?, ?)
            """,
            (transfer_date, company_name, recipient_bank, amount, company_fee),
        )
        conn.commit()
    finally:
        conn.close()


def get_directory_machine_info(machine_name):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        _create_tables(conn)
        row = conn.execute(
            """
            SELECT company_name, company_fee
            FROM directory_entries
            WHERE pos_machine = ?
            ORDER BY rowid DESC
            LIMIT 1
            """,
            (machine_name,),
        ).fetchone()
        if row is None:
            return {'company_name': 'N/A', 'company_fee': 1.09}
        return {
            'company_name': row['company_name'] or 'N/A',
            'company_fee': row['company_fee'] if row['company_fee'] is not None else 1.09,
        }
    finally:
        conn.close()


def add_owner_name(owner_name):
    conn = sqlite3.connect(DB_FILE)
    try:
        _create_tables(conn)
        existing = conn.execute(
            "SELECT 1 FROM directory_entries WHERE owner_name = ? LIMIT 1",
            (owner_name,),
        ).fetchone()
        if existing is None:
            conn.execute(
                "INSERT INTO directory_entries (owner_name) VALUES (?)",
                (owner_name,),
            )
            conn.commit()
    finally:
        conn.close()


def add_company_name(company_name):
    conn = sqlite3.connect(DB_FILE)
    try:
        _create_tables(conn)
        existing = conn.execute(
            "SELECT 1 FROM directory_entries WHERE company_name = ? LIMIT 1",
            (company_name,),
        ).fetchone()
        if existing is None:
            conn.execute(
                "INSERT INTO directory_entries (company_name) VALUES (?)",
                (company_name,),
            )
            conn.commit()
    finally:
        conn.close()


def upsert_machine_entry(machine_name, company_name, default_fee, company_fee):
    conn = sqlite3.connect(DB_FILE)
    try:
        _create_tables(conn)
        single_fee = company_fee if company_fee is not None else default_fee
        default_fee = single_fee
        company_fee = single_fee
        existing = conn.execute(
            "SELECT rowid FROM directory_entries WHERE pos_machine = ? ORDER BY rowid DESC LIMIT 1",
            (machine_name,),
        ).fetchone()
        if existing is None:
            conn.execute(
                """
                INSERT INTO directory_entries (pos_machine, company_name, default_fee, company_fee)
                VALUES (?, ?, ?, ?)
                """,
                (machine_name, company_name, default_fee, company_fee),
            )
        else:
            conn.execute(
                """
                UPDATE directory_entries
                SET company_name = ?,
                    default_fee = ?,
                    company_fee = ?
                WHERE rowid = ?
                """,
                (company_name, default_fee, company_fee, existing[0]),
            )
        conn.commit()
    finally:
        conn.close()


def delete_directory_by_owner(owner_name):
    conn = sqlite3.connect(DB_FILE)
    try:
        _create_tables(conn)
        conn.execute("DELETE FROM directory_entries WHERE owner_name = ?", (owner_name,))
        conn.commit()
    finally:
        conn.close()


def delete_directory_by_machine(machine_name):
    conn = sqlite3.connect(DB_FILE)
    try:
        _create_tables(conn)
        conn.execute("DELETE FROM directory_entries WHERE pos_machine = ?", (machine_name,))
        conn.commit()
    finally:
        conn.close()


def delete_directory_by_company(company_name):
    conn = sqlite3.connect(DB_FILE)
    try:
        _create_tables(conn)
        conn.execute("DELETE FROM directory_entries WHERE company_name = ?", (company_name,))
        conn.commit()
    finally:
        conn.close()


def set_directory_entry_visibility(entry_type, entry_name, is_visible):
    entry_type = str(entry_type or '').strip()
    entry_name = str(entry_name or '').strip()
    if entry_type not in {'owner', 'company', 'machine'} or not entry_name:
        return
    conn = sqlite3.connect(DB_FILE)
    try:
        _create_tables(conn)
        if is_visible:
            conn.execute(
                "DELETE FROM hidden_directory_entries WHERE entry_type = ? AND entry_name = ?",
                (entry_type, entry_name),
            )
        else:
            conn.execute(
                """
                INSERT OR IGNORE INTO hidden_directory_entries (entry_type, entry_name)
                VALUES (?, ?)
                """,
                (entry_type, entry_name),
            )
        conn.commit()
    finally:
        conn.close()


def save_all_data(df1, df_dm, df_notes, df_ct):
    conn = sqlite3.connect(DB_FILE)
    try:
        _create_tables(conn)
        _replace_transactions(conn, _normalize_transactions(df1))
        _replace_directory(conn, _normalize_directory(df_dm))
        _replace_notes(conn, _normalize_notes(df_notes))
        _replace_transfers(conn, _normalize_transfers(df_ct))
        conn.commit()
    finally:
        conn.close()


def _create_tables(conn):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_date TEXT,
            month_name TEXT,
            day_of_month INTEGER,
            owner_name TEXT,
            company_name TEXT,
            pos_machine TEXT,
            batch_number TEXT,
            amount REAL DEFAULT 0,
            customer_fee_amount REAL DEFAULT 0,
            amount_after_customer_fee REAL DEFAULT 0,
            company_fee_amount REAL DEFAULT 0,
            amount_after_company_fee REAL DEFAULT 0,
            transferred_amount REAL DEFAULT 0,
            profit_loss REAL DEFAULT 0,
            customer_fee_percent TEXT,
            company_fee_percent TEXT,
            note TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS directory_entries (
            owner_name TEXT,
            company_name TEXT,
            pos_machine TEXT,
            default_fee REAL DEFAULT 0,
            company_fee REAL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS hidden_directory_entries (
            entry_type TEXT NOT NULL,
            entry_name TEXT NOT NULL,
            PRIMARY KEY (entry_type, entry_name)
        );

        CREATE TABLE IF NOT EXISTS daily_notes (
            note_key TEXT PRIMARY KEY,
            note_text TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS company_transfers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transfer_date TEXT,
            company_name TEXT,
            recipient_bank TEXT DEFAULT '',
            transfer_amount REAL DEFAULT 0,
            company_fee REAL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS retail_customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            due_date TEXT,
            customer_name TEXT,
            card_number TEXT,
            bank_name TEXT DEFAULT '',
            processing_amount REAL DEFAULT 0,
            fee_percent REAL DEFAULT 0,
            card_adjustment REAL DEFAULT 0,
            current_debt REAL DEFAULT 0,
            added_debt REAL DEFAULT 0,
            paid_amount REAL DEFAULT 0,
            fee REAL DEFAULT 0,
            payment_status TEXT DEFAULT 'Nợ',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS summary_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            summary_date TEXT,
            customer_name TEXT,
            transaction_note TEXT DEFAULT '',
            amount REAL DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_transactions_report_chu
        ON transactions (transaction_date, owner_name, pos_machine, batch_number);

        CREATE INDEX IF NOT EXISTS idx_transactions_owner_machine
        ON transactions (owner_name, pos_machine);

        CREATE INDEX IF NOT EXISTS idx_transactions_company_date
        ON transactions (company_name, transaction_date);

        CREATE INDEX IF NOT EXISTS idx_company_transfers_company_date
        ON company_transfers (company_name, transfer_date);

        CREATE INDEX IF NOT EXISTS idx_retail_customers_due_status
        ON retail_customers (due_date, payment_status);

        CREATE INDEX IF NOT EXISTS idx_summary_entries_date
        ON summary_entries (summary_date);
        """
    )
    _ensure_column(conn, 'company_transfers', 'recipient_bank', "recipient_bank TEXT DEFAULT ''")
    _ensure_column(conn, 'retail_customers', 'bank_name', "bank_name TEXT DEFAULT ''")
    _ensure_column(conn, 'retail_customers', 'processing_amount', "processing_amount REAL DEFAULT 0")
    _ensure_column(conn, 'retail_customers', 'fee_percent', "fee_percent REAL DEFAULT 0")
    _ensure_column(conn, 'retail_customers', 'card_adjustment', "card_adjustment REAL DEFAULT 0")
    _ensure_column(conn, 'retail_customers', 'current_debt', "current_debt REAL DEFAULT 0")
    _ensure_column(conn, 'retail_customers', 'added_debt', "added_debt REAL DEFAULT 0")
    _ensure_column(conn, 'retail_customers', 'paid_amount', "paid_amount REAL DEFAULT 0")
    _ensure_column(conn, 'summary_entries', 'transaction_note', "transaction_note TEXT DEFAULT ''")


def _get_hidden_directory_sets(conn):
    _create_tables(conn)
    hidden = {'owner': set(), 'company': set(), 'machine': set()}
    rows = conn.execute(
        """
        SELECT entry_type, entry_name
        FROM hidden_directory_entries
        WHERE entry_type IN ('owner', 'company', 'machine')
        """
    ).fetchall()
    for row in rows:
        hidden[row['entry_type']].add(row['entry_name'])
    return hidden
    conn.commit()


def _ensure_column(conn, table_name, column_name, column_definition):
    columns = [row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()]
    if column_name not in columns:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_definition}")


def _normalize_transactions(raw_df):
    df = raw_df.copy() if isinstance(raw_df, pd.DataFrame) else pd.DataFrame()
    df = df.replace(r'^\s*$', pd.NA, regex=True)

    if 'id' not in df.columns:
        df['id'] = pd.NA

    legacy_fee = _series_to_numeric(df.get('Tiền phí'))
    transaction_amount = _series_to_numeric(df.get('Số tiền'))
    after_customer_fee = _series_to_numeric(df.get('Thành tiền sau phí'))
    customer_fee = _series_to_numeric(df.get('Tiền phí khách'))

    use_legacy_fee = customer_fee.fillna(0).eq(0) & legacy_fee.fillna(0).ne(0)
    customer_fee = customer_fee.where(~use_legacy_fee, legacy_fee)

    derive_customer_fee = (
        customer_fee.fillna(0).eq(0)
        & transaction_amount.fillna(0).ne(0)
        & after_customer_fee.fillna(0).ne(0)
        & transaction_amount.fillna(0).ne(after_customer_fee.fillna(0))
    )
    customer_fee = customer_fee.where(~derive_customer_fee, transaction_amount - after_customer_fee)

    customer_fee_percent = _coalesce_series(df, ['Biểu phí khách %', 'Biểu phí'])

    normalized = pd.DataFrame(
        {
            'id': pd.to_numeric(df['id'], errors='coerce').astype('Int64'),
            'Chi tiết ngày tháng': _coalesce_series(df, ['Chi tiết ngày tháng']).fillna(''),
            'Tháng': _coalesce_series(df, ['Tháng']),
            'Ngày': _series_to_numeric(df.get('Ngày')),
            'Tên Chủ POS': _coalesce_series(df, ['Tên Chủ POS']).fillna(''),
            'Tên Công ty': _coalesce_series(df, ['Tên Công ty']).fillna('Chưa gán'),
            'Máy POS': _coalesce_series(df, ['Máy POS']).fillna(''),
            'Số Lô': _coalesce_series(df, ['Số Lô']).fillna(''),
            'Số tiền': transaction_amount,
            'Tiền phí khách': customer_fee,
            'Thành tiền sau phí': after_customer_fee,
            'Phí trả Cty': _series_to_numeric(df.get('Phí trả Cty')),
            'Sau phí Cty': _series_to_numeric(df.get('Sau phí Cty')),
            'Thực chuyển': _series_to_numeric(df.get('Thực chuyển')),
            'Lãi/Lỗ': _series_to_numeric(df.get('Lãi/Lỗ')),
            'Biểu phí khách %': customer_fee_percent.fillna(''),
            'Phí Cty %': _coalesce_series(df, ['Phí Cty %']).fillna(''),
            'Ghi chú': _coalesce_series(df, ['Ghi chú']).fillna(''),
        }
    )

    parsed_dates = pd.to_datetime(normalized['Chi tiết ngày tháng'], errors='coerce')
    missing_month = normalized['Tháng'].isna() | normalized['Tháng'].astype(str).str.strip().eq('')
    normalized.loc[missing_month, 'Tháng'] = parsed_dates.dt.strftime('%b')
    normalized['Ngày'] = normalized['Ngày'].fillna(parsed_dates.dt.day)

    for col in NUMERIC_TRANSACTION_COLUMNS:
        normalized[col] = pd.to_numeric(normalized[col], errors='coerce').fillna(0)

    normalized['Chi tiết ngày tháng'] = normalized['Chi tiết ngày tháng'].fillna('').astype(str)
    normalized['Tên Công ty'] = normalized['Tên Công ty'].fillna('Chưa gán')
    return normalized[['id'] + TRANSACTION_COLUMNS]


def _normalize_directory(raw_df):
    df = raw_df.copy() if isinstance(raw_df, pd.DataFrame) else pd.DataFrame()
    df = df.replace(r'^\s*$', pd.NA, regex=True)

    normalized = pd.DataFrame({col: df[col] if col in df.columns else pd.NA for col in DIRECTORY_COLUMNS})
    for col in NUMERIC_DIRECTORY_COLUMNS:
        normalized[col] = pd.to_numeric(normalized[col], errors='coerce')
    return normalized[DIRECTORY_COLUMNS]


def _normalize_notes(raw_df):
    df = raw_df.copy() if isinstance(raw_df, pd.DataFrame) else pd.DataFrame()
    df = df.replace(r'^\s*$', pd.NA, regex=True)
    normalized = pd.DataFrame({col: df[col] if col in df.columns else pd.NA for col in NOTE_COLUMNS})
    normalized = normalized.dropna(subset=['Key'])
    normalized['Note'] = normalized['Note'].fillna('')
    return normalized[NOTE_COLUMNS]


def _normalize_transfers(raw_df):
    df = raw_df.copy() if isinstance(raw_df, pd.DataFrame) else pd.DataFrame()
    df = df.replace(r'^\s*$', pd.NA, regex=True)

    if 'id' not in df.columns:
        df['id'] = pd.NA

    normalized = pd.DataFrame(
        {
            'id': pd.to_numeric(df['id'], errors='coerce').astype('Int64'),
            'Ngày tháng': df['Ngày tháng'] if 'Ngày tháng' in df.columns else pd.NA,
            'Tên Công ty': df['Tên Công ty'] if 'Tên Công ty' in df.columns else pd.NA,
            'Ngân hàng nhận': df['Ngân hàng nhận'] if 'Ngân hàng nhận' in df.columns else pd.NA,
            'Số tiền chuyển': _series_to_numeric(df.get('Số tiền chuyển')),
            'Phí công ty': _series_to_numeric(df.get('Phí công ty')),
        }
    )

    for col in NUMERIC_TRANSFER_COLUMNS:
        normalized[col] = pd.to_numeric(normalized[col], errors='coerce').fillna(0)

    normalized['Ngày tháng'] = normalized['Ngày tháng'].fillna('').astype(str)
    normalized['Tên Công ty'] = normalized['Tên Công ty'].fillna('')
    normalized['Ngân hàng nhận'] = normalized['Ngân hàng nhận'].fillna('').astype(str)
    return normalized[['id'] + TRANSFER_COLUMNS]


def _replace_transactions(conn, df):
    normalized = _normalize_transactions(df)
    conn.execute('DELETE FROM transactions')
    insert_columns = [TRANSACTION_DB_MAP[col] for col in ['id'] + TRANSACTION_COLUMNS]
    placeholders = ', '.join(['?'] * len(insert_columns))
    sql = f"INSERT INTO transactions ({', '.join(insert_columns)}) VALUES ({placeholders})"
    conn.executemany(sql, _dataframe_to_records(normalized, ['id'] + TRANSACTION_COLUMNS))


def _replace_directory(conn, df):
    normalized = _normalize_directory(df)
    conn.execute('DELETE FROM directory_entries')
    insert_columns = [DIRECTORY_DB_MAP[col] for col in DIRECTORY_COLUMNS]
    placeholders = ', '.join(['?'] * len(insert_columns))
    sql = f"INSERT INTO directory_entries ({', '.join(insert_columns)}) VALUES ({placeholders})"
    conn.executemany(sql, _dataframe_to_records(normalized, DIRECTORY_COLUMNS))


def _replace_notes(conn, df):
    normalized = _normalize_notes(df)
    conn.execute('DELETE FROM daily_notes')
    insert_columns = [NOTE_DB_MAP[col] for col in NOTE_COLUMNS]
    placeholders = ', '.join(['?'] * len(insert_columns))
    sql = f"INSERT INTO daily_notes ({', '.join(insert_columns)}) VALUES ({placeholders})"
    conn.executemany(sql, _dataframe_to_records(normalized, NOTE_COLUMNS))


def _replace_transfers(conn, df):
    normalized = _normalize_transfers(df)
    conn.execute('DELETE FROM company_transfers')
    insert_columns = [TRANSFER_DB_MAP[col] for col in ['id'] + TRANSFER_COLUMNS]
    placeholders = ', '.join(['?'] * len(insert_columns))
    sql = f"INSERT INTO company_transfers ({', '.join(insert_columns)}) VALUES ({placeholders})"
    conn.executemany(sql, _dataframe_to_records(normalized, ['id'] + TRANSFER_COLUMNS))


def _dataframe_to_records(df, columns):
    safe_df = df[columns].where(pd.notna(df[columns]), None)
    records = []
    for row in safe_df.to_dict('records'):
        records.append(tuple(row[col] for col in columns))
    return records


def _coalesce_series(df, columns):
    series = pd.Series([pd.NA] * len(df), index=df.index, dtype='object')
    for col in columns:
        if col in df.columns:
            series = series.fillna(df[col])
    return series


def _series_to_numeric(series):
    if series is None:
        return pd.Series(dtype='float64')
    return pd.to_numeric(series, errors='coerce')


def _build_transaction_filters(filter_from_date='', filter_to_date='', filter_owner='Tất cả', filter_machine='Tất cả'):
    clauses = []
    params = []
    _append_date_range_filters(clauses, params, 'transaction_date', filter_from_date, filter_to_date)
    if filter_owner != 'Tất cả':
        clauses.append('owner_name = ?')
        params.append(filter_owner)
    if filter_machine != 'Tất cả':
        clauses.append('pos_machine = ?')
        params.append(filter_machine)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ''
    return where_sql, params


def _append_date_range_filters(clauses, params, column_name, filter_from_date='', filter_to_date=''):
    if filter_from_date and filter_to_date:
        clauses.append(f'{column_name} BETWEEN ? AND ?')
        params.extend([filter_from_date, filter_to_date])
    elif filter_from_date:
        clauses.append(f'{column_name} >= ?')
        params.append(filter_from_date)
    elif filter_to_date:
        clauses.append(f'{column_name} <= ?')
        params.append(filter_to_date)


def _append_not_in_clause(clauses, params, column_name, values):
    values = sorted(value for value in values if value)
    if not values:
        return
    placeholders = ', '.join(['?'] * len(values))
    clauses.append(f'{column_name} NOT IN ({placeholders})')
    params.extend(values)


def _append_hidden_company_clause(clauses, params, hidden):
    _append_not_in_clause(clauses, params, 'company_name', hidden['company'])


def _extend_where_sql(where_sql, params, clauses, clause_params):
    if not clauses:
        return where_sql, params
    joiner = ' AND ' if where_sql else 'WHERE '
    return f"{where_sql}{joiner}{' AND '.join(clauses)}", [*params, *clause_params]


def _apply_hidden_transaction_filters(where_sql, params, hidden):
    clauses = []
    clause_params = []
    _append_not_in_clause(clauses, clause_params, 'owner_name', hidden['owner'])
    _append_not_in_clause(clauses, clause_params, 'company_name', hidden['company'])
    _append_not_in_clause(clauses, clause_params, 'pos_machine', hidden['machine'])
    return _extend_where_sql(where_sql, params, clauses, clause_params)


def _apply_hidden_company_filters(where_sql, params, hidden):
    clauses = []
    clause_params = []
    _append_hidden_company_clause(clauses, clause_params, hidden)
    return _extend_where_sql(where_sql, params, clauses, clause_params)


def _build_company_transaction_filters(filter_from_date='', filter_to_date='', filter_company='Tất cả', filter_machine='Tất cả'):
    clauses = []
    params = []
    _append_date_range_filters(clauses, params, 'transaction_date', filter_from_date, filter_to_date)
    if filter_company != 'Tất cả':
        clauses.append('company_name = ?')
        params.append(filter_company)
    if filter_machine != 'Tất cả':
        clauses.append('pos_machine = ?')
        params.append(filter_machine)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ''
    return where_sql, params


def _build_company_transfer_filters(filter_from_date='', filter_to_date='', filter_company='Tất cả'):
    clauses = []
    params = []
    _append_date_range_filters(clauses, params, 'transfer_date', filter_from_date, filter_to_date)
    if filter_company != 'Tất cả':
        clauses.append('company_name = ?')
        params.append(filter_company)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ''
    return where_sql, params
