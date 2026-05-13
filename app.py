import json
import logging
import os
import time
from flask import Flask, g, render_template, request, redirect, url_for
import pandas as pd
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from storage import (
    add_summary_entry,
    add_company_name,
    add_company_transfer_entry,
    add_owner_name,
    add_retail_customer_entry,
    add_transfer_to_transaction,
    delete_company_transfer_by_id,
    delete_directory_by_company,
    delete_directory_by_machine,
    delete_directory_by_owner,
    delete_retail_customer_by_id,
    delete_summary_entry_by_id,
    delete_transaction_by_id,
    get_all_sheets,
    get_all_transactions_context,
    get_company_transfers_context,
    get_customer_daily_summary,
    get_directory_context,
    get_directory_machine_info,
    get_report_chu_context,
    get_report_cty_context,
    get_retail_customers_context,
    get_transaction_by_id,
    init_database,
    save_all_data,
    set_directory_entry_visibility,
    update_retail_customer_entry,
    update_summary_entry_by_id,
    update_transaction_by_id,
    upsert_machine_entry,
    upsert_note,
)

app = Flask(__name__)
app.logger.setLevel(logging.INFO)

try:
    APP_TIMEZONE = ZoneInfo(os.environ.get('APP_TIMEZONE', 'Asia/Ho_Chi_Minh'))
except ZoneInfoNotFoundError:
    APP_TIMEZONE = timezone(timedelta(hours=7))


def read_bool_env(name, default=False):
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {'1', 'true', 'yes', 'on'}


def parse_float_input(value, default=0):
    text = str(value or '').strip().replace(',', '').replace('%', '')
    if not text:
        return default
    try:
        return float(text)
    except ValueError:
        return default


def clean_money(value):
    return 0 if abs(value) < 0.5 else round(value, 6)


def calculate_retail_fee_from_form(form):
    processing_amount = parse_float_input(form.get('processing_amount'), 0)
    fee_percent = parse_float_input(form.get('fee_percent'), 0)
    card_adjustment = parse_float_input(form.get('card_adjustment'), 0)
    if processing_amount or fee_percent or card_adjustment:
        fee = clean_money((processing_amount * fee_percent / 100) + card_adjustment)
    else:
        fee = parse_float_input(form.get('fee'), 0)
    return processing_amount, fee_percent, card_adjustment, fee


def calculate_retail_debt_from_form(form):
    current_debt = parse_float_input(form.get('current_debt'), 0)
    added_debt = parse_float_input(form.get('added_debt'), 0)
    paid_amount = parse_float_input(form.get('paid_amount'), 0)
    remaining_debt = max(clean_money(current_debt + added_debt - paid_amount), 0)
    return current_debt, added_debt, paid_amount, remaining_debt


def log_route_breakdown(route_name, **phases_ms):
    if not phases_ms:
        return
    app.logger.info(
        "%s breakdown: %s",
        route_name,
        ", ".join(f"{name}={value:.1f}ms" for name, value in phases_ms.items()),
    )


def normalize_start_url(raw_value):
    parts = urlsplit((raw_value or '').strip() or '/')
    path = parts.path or '/'
    if not path.startswith('/'):
        path = f'/{path}'
    return urlunsplit(('', '', path, '', ''))


def current_install_start_url():
    return normalize_start_url(request.path)


def today_iso():
    return datetime.now(APP_TIMEZONE).strftime('%Y-%m-%d')


def current_time_hm():
    return datetime.now(APP_TIMEZONE).strftime('%H:%M')


def parse_summary_amount_from_form(form):
    amount = abs(parse_float_input(form.get('summary_amount'), 0))
    operation = form.get('summary_operation')
    if operation == 'subtract':
        amount = -amount
    return amount, operation


def parse_summary_created_at_from_form(form, summary_date):
    raw_time = str(form.get('summary_time') or '').strip()
    try:
        parsed_time = datetime.strptime(raw_time, '%H:%M').strftime('%H:%M')
    except ValueError:
        parsed_time = current_time_hm()
    date_value = normalize_filter_date(summary_date) or today_iso()
    return f'{date_value} {parsed_time}:00'


def normalize_filter_date(raw_value):
    value = str(raw_value or '').strip()
    if not value:
        return ''
    for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y'):
        try:
            return datetime.strptime(value, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return ''


def resolve_date_range(args, date_key='date', from_key='from_date', to_key='to_date', default_to_today=True):
    legacy_date = normalize_filter_date(args.get(date_key))
    from_date = normalize_filter_date(args.get(from_key))
    to_date = normalize_filter_date(args.get(to_key))

    if legacy_date and not from_date and not to_date:
        from_date = legacy_date
        to_date = legacy_date

    if default_to_today and not from_date and not to_date:
        today = today_iso()
        from_date = today
        to_date = today

    if from_date and to_date and from_date > to_date:
        from_date, to_date = to_date, from_date

    return from_date, to_date


@app.context_processor
def inject_pwa_manifest_url():
    return {
        'pwa_manifest_url': url_for('web_manifest', start_url=current_install_start_url()),
    }


@app.before_request
def start_request_timer():
    g.request_started_at = time.perf_counter()


@app.after_request
def log_request_timing(response):
    started_at = getattr(g, 'request_started_at', None)
    if started_at is not None:
        duration_ms = (time.perf_counter() - started_at) * 1000
        app.logger.info(
            "%s %s -> %s in %.1fms",
            request.method,
            request.full_path if request.query_string else request.path,
            response.status_code,
            duration_ms,
        )
    return response

# --- 2. ROUTES TRANG CHỦ & NHẬP LIỆU ---

@app.route('/')
def index():
    started_at = time.perf_counter()
    context = get_directory_context()
    context_done_at = time.perf_counter()
    response = render_template(
        'index.html',
        chu_pos_list=context['chu_pos_list'],
        companies=context['companies'],
        may_pos_data=context['may_pos'],
    )
    render_done_at = time.perf_counter()
    log_route_breakdown(
        'index',
        context_ms=(context_done_at - started_at) * 1000,
        render_ms=(render_done_at - context_done_at) * 1000,
    )
    return response


@app.route('/summary')
def summary():
    started_at = time.perf_counter()
    has_summary_filters = bool(request.args)
    requested_date = normalize_filter_date(request.args.get('date'))
    filter_date = requested_date
    if not has_summary_filters:
        filter_date = today_iso()
    filters = {
        'date': requested_date,
        'item': request.args.get('item', '').strip(),
        'text': request.args.get('q', '').strip(),
    }
    customer_summary = get_customer_daily_summary(
        filter_date=filter_date,
        filter_item=filters['item'],
        filter_text=filters['text'],
    )
    context_done_at = time.perf_counter()
    response = render_template(
        'summary.html',
        customer_summary=customer_summary,
        today=today_iso(),
        now_time=current_time_hm(),
        filters=filters,
    )
    render_done_at = time.perf_counter()
    log_route_breakdown(
        'summary',
        context_ms=(context_done_at - started_at) * 1000,
        render_ms=(render_done_at - context_done_at) * 1000,
    )
    return response

@app.route('/manifest.webmanifest')
def web_manifest():
    start_url = normalize_start_url(request.args.get('start_url', '/'))
    manifest = {
        'id': start_url,
        'name': 'Quan Ly Ngan Hang',
        'short_name': 'QuanLyNH',
        'description': 'Quan ly giao dich POS tren iPhone',
        'start_url': start_url,
        'scope': '/',
        'display': 'standalone',
        'background_color': '#eef4ff',
        'theme_color': '#0d6efd',
        'icons': [
            {
                'src': '/static/finance-icon-180.png',
                'sizes': '180x180',
                'type': 'image/png',
                'purpose': 'any',
            },
            {
                'src': '/static/finance-icon-512.png',
                'sizes': '512x512',
                'type': 'image/png',
                'purpose': 'any',
            }
        ],
    }
    response = app.response_class(
        response=json.dumps(manifest, ensure_ascii=True),
        mimetype='application/manifest+json',
    )
    response.headers['Cache-Control'] = 'no-store, max-age=0'
    return response


@app.route('/add_summary_entry', methods=['POST'])
def add_summary_entry_route():
    started_at = time.perf_counter()
    summary_date = normalize_filter_date(request.form.get('summary_date'))
    customer_name = request.form.get('summary_customer', '').strip()
    transaction_note = request.form.get('summary_transaction', '').strip()
    amount, operation = parse_summary_amount_from_form(request.form)
    created_at = parse_summary_created_at_from_form(request.form, summary_date)
    if not transaction_note and amount:
        action_label = 'Trừ' if operation == 'subtract' else 'Cộng'
        transaction_note = f"{action_label} nhanh {abs(amount):,.0f}"
    if summary_date and customer_name:
        add_summary_entry(summary_date, customer_name, amount, transaction_note, created_at)
    write_done_at = time.perf_counter()
    response = redirect(url_for('summary', date=summary_date))
    redirect_done_at = time.perf_counter()
    log_route_breakdown(
        'add_summary_entry',
        write_ms=(write_done_at - started_at) * 1000,
        redirect_ms=(redirect_done_at - write_done_at) * 1000,
    )
    return response


@app.route('/update_summary_entry', methods=['POST'])
def update_summary_entry_route():
    started_at = time.perf_counter()
    try:
        entry_id = int(request.form.get('summary_entry_id') or 0)
    except ValueError:
        entry_id = 0
    summary_date = normalize_filter_date(request.form.get('summary_date'))
    customer_name = request.form.get('summary_customer', '').strip()
    transaction_note = request.form.get('summary_transaction', '').strip()
    amount, operation = parse_summary_amount_from_form(request.form)
    created_at = parse_summary_created_at_from_form(request.form, summary_date)
    if not transaction_note and amount:
        action_label = 'Trừ' if operation == 'subtract' else 'Cộng'
        transaction_note = f"{action_label} nhanh {abs(amount):,.0f}"
    if entry_id and summary_date and customer_name:
        update_summary_entry_by_id(entry_id, summary_date, customer_name, amount, transaction_note, created_at)
    write_done_at = time.perf_counter()
    response = redirect(url_for('summary', date=summary_date))
    redirect_done_at = time.perf_counter()
    log_route_breakdown(
        'update_summary_entry',
        write_ms=(write_done_at - started_at) * 1000,
        redirect_ms=(redirect_done_at - write_done_at) * 1000,
    )
    return response


@app.route('/delete_summary_entry/<int:entry_id>')
def delete_summary_entry(entry_id):
    started_at = time.perf_counter()
    ret_date = normalize_filter_date(request.args.get('ret_date'))
    delete_summary_entry_by_id(entry_id)
    write_done_at = time.perf_counter()
    response = redirect(url_for('summary', date=ret_date) if ret_date else url_for('summary'))
    redirect_done_at = time.perf_counter()
    log_route_breakdown(
        'delete_summary_entry',
        write_ms=(write_done_at - started_at) * 1000,
        redirect_ms=(redirect_done_at - write_done_at) * 1000,
    )
    return response


@app.route('/submit', methods=['POST'])
def submit():
    started_at = time.perf_counter()
    df1, df_dm, df_notes, df_ct = get_all_sheets()
    load_done_at = time.perf_counter()
    so_tien = float(request.form['doanh_thu'].replace(',', '') or 0)
    sau_phi_k = float(request.form['ck_khach'].replace(',', '') or 0)
    thuc_chuyen = parse_float_input(request.form.get('thuc_chuyen'), 0)
    ten_may, phi_k_pct = request.form['may_pos'], float(request.form['bieu_phi'] or 0)
    m_info = df_dm[df_dm['Máy POS'] == ten_may]
    cty = m_info['Tên Công ty'].values[0] if not m_info.empty else request.form.get('ten_cong_ty', 'N/A')
    owner_name = m_info['Tên Chủ POS'].values[0] if not m_info.empty else ''
    phi_c_pct = phi_k_pct
    p_k_v = clean_money(so_tien - sau_phi_k)
    p_c_v = clean_money(so_tien * (phi_c_pct / 100))
    dt_obj = datetime.strptime(request.form['chi_tiet_ngay'], '%Y-%m-%d')
    new_row = {
        'Chi tiết ngày tháng': request.form['chi_tiet_ngay'], 'Tháng': dt_obj.strftime('%b'), 'Ngày': dt_obj.day,
        'Tên Chủ POS': owner_name, 'Tên Công ty': cty, 'Máy POS': ten_may, 'Số Lô': request.form['so_lo'],
        'Số tiền': so_tien, 'Tiền phí khách': p_k_v, 'Thành tiền sau phí': sau_phi_k, 
        'Phí trả Cty': p_c_v, 'Sau phí Cty': so_tien - p_c_v, 'Thực chuyển': thuc_chuyen, 
        'Lãi/Lỗ': clean_money(p_k_v - p_c_v), 'Biểu phí khách %': f"{phi_k_pct}%", 'Phí Cty %': f"{phi_c_pct}%", 'Ghi chú': ""
    }
    compute_done_at = time.perf_counter()
    df1 = pd.concat([df1, pd.DataFrame([new_row])], ignore_index=True)
    save_all_data(df1, df_dm, df_notes, df_ct)
    save_done_at = time.perf_counter()
    response = redirect(url_for('report_cty', date=request.form['chi_tiet_ngay']))
    redirect_done_at = time.perf_counter()
    log_route_breakdown(
        'submit',
        load_ms=(load_done_at - started_at) * 1000,
        compute_ms=(compute_done_at - load_done_at) * 1000,
        save_ms=(save_done_at - compute_done_at) * 1000,
        redirect_ms=(redirect_done_at - save_done_at) * 1000,
    )
    return response

# --- 3. BÁO CÁO CHỦ POS ---

@app.route('/report_chu')
def report_chu():
    f_from_date, f_to_date = resolve_date_range(request.args)
    return redirect(url_for(
        'report_cty',
        from_date=f_from_date,
        to_date=f_to_date,
        cong_ty=request.args.get('cong_ty', 'Tất cả'),
    ))

# --- 4. BÁO CÁO CÔNG TY ---

@app.route('/report_cty')
def report_cty():
    started_at = time.perf_counter()
    f_from_date, f_to_date = resolve_date_range(request.args)
    f_cty = request.args.get('cong_ty', 'Tất cả')
    f_may = request.args.get('may_pos', 'Tất cả')
    context = get_report_cty_context(f_from_date, f_to_date, f_cty, f_may)
    visible_owner_count = len({
        tx['owner']
        for item in context['report_data']
        for tx in item.get('transactions', [])
        if tx.get('owner')
    })
    context_done_at = time.perf_counter()
    response = render_template(
        'report_cty.html',
        report_data=context['report_data'],
        list_cty=context['list_cty'],
        list_may=context['list_may'],
        sel_cty=f_cty,
        sel_may=f_may,
        sel_date=f_from_date if f_from_date == f_to_date else '',
        sel_from_date=f_from_date,
        sel_to_date=f_to_date,
        show_owner_column=visible_owner_count > 1,
    )
    render_done_at = time.perf_counter()
    log_route_breakdown(
        'report_cty',
        context_ms=(context_done_at - started_at) * 1000,
        render_ms=(render_done_at - context_done_at) * 1000,
    )
    return response

# --- 5. QUẢN LÝ TIỀN BANK VỀ ---

@app.route('/company_transfers')
def company_transfers():
    started_at = time.perf_counter()
    f_from_date, f_to_date = resolve_date_range(request.args)
    f_cty, f_amt = request.args.get('company', 'Tất cả'), request.args.get('amount', '')
    context = get_company_transfers_context(f_from_date, f_to_date, f_cty, f_amt)
    context_done_at = time.perf_counter()
    response = render_template(
        'company_transfers.html',
        transfers=context['transfers'],
        companies=context['companies'],
        sel_date=f_from_date if f_from_date == f_to_date else '',
        sel_from_date=f_from_date,
        sel_to_date=f_to_date,
        sel_cty=f_cty,
        sel_amt=f_amt,
        total_amt=context['total_amt'],
    )
    render_done_at = time.perf_counter()
    log_route_breakdown(
        'company_transfers',
        context_ms=(context_done_at - started_at) * 1000,
        render_ms=(render_done_at - context_done_at) * 1000,
    )
    return response

@app.route('/add_company_transfer', methods=['POST'])
def add_company_transfer():
    started_at = time.perf_counter()
    add_company_transfer_entry(
        request.form['date'],
        request.form['company'],
        parse_float_input(request.form.get('amount', '0')),
        parse_float_input(request.form.get('fee', '0')),
        request.form.get('recipient_bank', '').strip(),
    )
    write_done_at = time.perf_counter()
    response = redirect(url_for('company_transfers'))
    redirect_done_at = time.perf_counter()
    log_route_breakdown(
        'add_company_transfer',
        write_ms=(write_done_at - started_at) * 1000,
        redirect_ms=(redirect_done_at - write_done_at) * 1000,
    )
    return response

# --- 6. QUẢN LÝ KHÁCH HÀNG LẺ ĐÁO HẠN THẺ ---

@app.route('/retail_customers')
def retail_customers():
    started_at = time.perf_counter()
    f_from_due, f_to_due = resolve_date_range(
        request.args,
        date_key='due_date',
        from_key='from_due_date',
        to_key='to_due_date',
        default_to_today=False,
    )
    f_status = request.args.get('status', 'Tất cả')
    f_customer = request.args.get('customer', '').strip()
    f_card = request.args.get('card', '').strip()
    context = get_retail_customers_context(f_from_due, f_to_due, f_status, f_customer, f_card)
    context_done_at = time.perf_counter()
    response = render_template(
        'retail_customers.html',
        customers=context['customers'],
        customer_names=context['customer_names'],
        bank_names=context['bank_names'],
        summary=context['summary'],
        sel_from_due=f_from_due,
        sel_to_due=f_to_due,
        sel_status=f_status,
        sel_customer=f_customer,
        sel_card=f_card,
    )
    render_done_at = time.perf_counter()
    log_route_breakdown(
        'retail_customers',
        context_ms=(context_done_at - started_at) * 1000,
        render_ms=(render_done_at - context_done_at) * 1000,
    )
    return response


@app.route('/add_retail_customer', methods=['POST'])
def add_retail_customer():
    started_at = time.perf_counter()
    customer_name = request.form.get('customer_name', '').strip()
    due_date = request.form.get('due_date', '').strip()
    current_debt, added_debt, paid_amount, remaining_debt = calculate_retail_debt_from_form(request.form)
    if customer_name and due_date:
        add_retail_customer_entry(
            due_date,
            customer_name,
            current_debt=current_debt,
            added_debt=added_debt,
            paid_amount=paid_amount,
            fee=remaining_debt,
            payment_status='Nợ' if remaining_debt > 0 else 'Trả',
        )
    write_done_at = time.perf_counter()
    response = redirect(url_for('retail_customers'))
    redirect_done_at = time.perf_counter()
    log_route_breakdown(
        'add_retail_customer',
        write_ms=(write_done_at - started_at) * 1000,
        redirect_ms=(redirect_done_at - write_done_at) * 1000,
    )
    return response


@app.route('/update_retail_customer', methods=['POST'])
def update_retail_customer():
    started_at = time.perf_counter()
    customer_id = int(request.form.get('customer_id', 0))
    current_debt, added_debt, paid_amount, remaining_debt = calculate_retail_debt_from_form(request.form)
    if customer_id > 0:
        update_retail_customer_entry(
            customer_id,
            request.form.get('due_date', '').strip(),
            request.form.get('customer_name', '').strip(),
            current_debt=current_debt,
            added_debt=added_debt,
            paid_amount=paid_amount,
            fee=remaining_debt,
            payment_status='Nợ' if remaining_debt > 0 else 'Trả',
        )
    write_done_at = time.perf_counter()
    response = redirect(request.referrer or url_for('retail_customers'))
    redirect_done_at = time.perf_counter()
    log_route_breakdown(
        'update_retail_customer',
        write_ms=(write_done_at - started_at) * 1000,
        redirect_ms=(redirect_done_at - write_done_at) * 1000,
    )
    return response


@app.route('/delete_retail_customer/<int:customer_id>')
def delete_retail_customer(customer_id):
    started_at = time.perf_counter()
    delete_retail_customer_by_id(customer_id)
    write_done_at = time.perf_counter()
    response = redirect(request.referrer or url_for('retail_customers'))
    redirect_done_at = time.perf_counter()
    log_route_breakdown(
        'delete_retail_customer',
        write_ms=(write_done_at - started_at) * 1000,
        redirect_ms=(redirect_done_at - write_done_at) * 1000,
    )
    return response


# --- 7. HÀNH ĐỘNG SỬA / XÓA / CÀI ĐẶT ---

@app.route('/settings')
def settings():
    started_at = time.perf_counter()
    context = get_directory_context()
    context_done_at = time.perf_counter()
    response = render_template(
        'settings.html',
        chu_pos=context['all_chu_pos'],
        companies=context['all_companies'],
        visible_companies=context['companies'],
        may_pos=context['all_may_pos'],
    )
    render_done_at = time.perf_counter()
    log_route_breakdown(
        'settings',
        context_ms=(context_done_at - started_at) * 1000,
        render_ms=(render_done_at - context_done_at) * 1000,
    )
    return response

@app.route('/add_chu_pos', methods=['POST'])
def add_chu_pos():
    n = request.form['new_chu_pos'].strip().upper()
    if n:
        add_owner_name(n)
    return redirect(url_for('settings'))

@app.route('/add_company', methods=['POST'])
def add_company():
    n = request.form['new_company'].strip().upper()
    if n:
        add_company_name(n)
    return redirect(url_for('settings'))

@app.route('/add_may_pos', methods=['POST'])
def add_may_pos():
    may = request.form['new_may_pos'].strip()
    cty = request.form['belong_company']
    phi_pos = parse_float_input(request.form.get('new_phi_cty') or request.form.get('new_bieu_phi'), 1.09)
    if may:
        upsert_machine_entry(may, cty, phi_pos, phi_pos)
    return redirect(url_for('settings'))


@app.route('/toggle_directory_visibility', methods=['POST'])
def toggle_directory_visibility():
    entry_type = request.form.get('entry_type', '')
    entry_name = request.form.get('entry_name', '')
    is_visible = request.form.get('visible') == '1'
    set_directory_entry_visibility(entry_type, entry_name, is_visible)
    return redirect(url_for('settings'))

@app.route('/edit_transaction/<int:idx>')
def edit_transaction(idx):
    started_at = time.perf_counter()
    row = get_transaction_by_id(idx)
    row_done_at = time.perf_counter()
    if row is None:
        return redirect(url_for('all_transactions'))
    directory = get_directory_context()
    context_done_at = time.perf_counter()
    row['index'] = idx
    ret_mode = request.args.get('ret_mode', 'report_chu')
    ret_date = request.args.get('ret_date', '')
    ret_from_date, ret_to_date = resolve_date_range(
        request.args,
        date_key='ret_date',
        from_key='ret_from_date',
        to_key='ret_to_date',
        default_to_today=False,
    )
    ret_chu = request.args.get('ret_chu', 'Tất cả')
    ret_cty = request.args.get('ret_cty', 'Tất cả')
    ret_may = request.args.get('ret_may', 'Tất cả')
    response = render_template(
        'edit_transaction.html',
        row=row,
        chu_pos_list=directory['chu_pos_list'],
        may_pos=directory['may_pos'],
        ret_mode=ret_mode,
        ret_date=ret_date,
        ret_from_date=ret_from_date,
        ret_to_date=ret_to_date,
        ret_chu=ret_chu,
        ret_cty=ret_cty,
        ret_may=ret_may
    )
    render_done_at = time.perf_counter()
    log_route_breakdown(
        'edit_transaction',
        row_ms=(row_done_at - started_at) * 1000,
        directory_ms=(context_done_at - row_done_at) * 1000,
        render_ms=(render_done_at - context_done_at) * 1000,
    )
    return response

@app.route('/update_transaction', methods=['POST'])
def update_transaction():
    started_at = time.perf_counter()
    idx = int(request.form['index'])
    current_row = get_transaction_by_id(idx)
    load_done_at = time.perf_counter()
    if current_row is not None:
        tx_date = request.form['chi_tiet_ngay']
        dt_obj = datetime.strptime(tx_date, '%Y-%m-%d')
        machine = request.form['may_pos']
        machine_info = get_directory_machine_info(machine)
        machine_done_at = time.perf_counter()
        phi_k = float(request.form['bieu_phi'] or 0)
        phi_c = phi_k
        so_tien = parse_float_input(request.form.get('so_tien'), current_row['Số tiền'])
        customer_fee_amount = clean_money(so_tien * (phi_k / 100))
        company_fee_amount = clean_money(so_tien * (phi_c / 100))
        compute_done_at = time.perf_counter()
        update_transaction_by_id(
            idx,
            {
                'transaction_date': tx_date,
                'month_name': dt_obj.strftime('%b'),
                'day_of_month': dt_obj.day,
                'owner_name': request.form['ten_chu_pos'],
                'company_name': machine_info['company_name'],
                'pos_machine': machine,
                'batch_number': request.form['so_lo'],
                'amount': so_tien,
                'customer_fee_amount': customer_fee_amount,
                'amount_after_customer_fee': clean_money(so_tien - customer_fee_amount),
                'company_fee_amount': company_fee_amount,
                'amount_after_company_fee': clean_money(so_tien - company_fee_amount),
                'transferred_amount': current_row['Thực chuyển'],
                'profit_loss': clean_money(customer_fee_amount - company_fee_amount),
                'customer_fee_percent': f"{phi_k}%",
                'company_fee_percent': f"{phi_c}%",
                'note': current_row['Ghi chú'],
            },
        )
        write_done_at = time.perf_counter()
    else:
        machine_done_at = load_done_at
        compute_done_at = load_done_at
        write_done_at = load_done_at
    ret_mode = request.form.get('ret_mode', 'report_chu')
    if ret_mode == 'all_transactions':
        response = redirect(url_for(
            'all_transactions',
            from_date=request.form.get('ret_from_date', ''),
            to_date=request.form.get('ret_to_date', ''),
            chu_pos=request.form.get('ret_chu', 'Tất cả'),
            cong_ty=request.form.get('ret_cty', 'Tất cả'),
            may_pos=request.form.get('ret_may', 'Tất cả')
        ))
    else:
        response = redirect(url_for(
            'report_cty',
            from_date=request.form.get('ret_from_date', ''),
            to_date=request.form.get('ret_to_date', ''),
            cong_ty=request.form.get('ret_cty', 'Tất cả'),
        ))
    redirect_done_at = time.perf_counter()
    log_route_breakdown(
        'update_transaction',
        load_ms=(load_done_at - started_at) * 1000,
        machine_ms=(machine_done_at - load_done_at) * 1000,
        compute_ms=(compute_done_at - machine_done_at) * 1000,
        write_ms=(write_done_at - compute_done_at) * 1000,
        redirect_ms=(redirect_done_at - write_done_at) * 1000,
    )
    return response

@app.route('/update_transaction_inline', methods=['POST'])
def update_transaction_inline():
    started_at = time.perf_counter()
    idx = int(request.form['index'])
    current_row = get_transaction_by_id(idx)
    load_done_at = time.perf_counter()
    if current_row is not None:
        tx_date = request.form.get('chi_tiet_ngay', '')
        phi_c = parse_float_input(request.form.get('phi_cty'), 0)
        phi_k = parse_float_input(request.form.get('bieu_phi_khach'), phi_c)
        so_tien = parse_float_input(request.form.get('so_tien'), 0)
        tien_phi_khach = clean_money(so_tien * (phi_k / 100))
        thanh_tien_sau_phi = clean_money(so_tien - tien_phi_khach)
        phi_tra_cty = clean_money(so_tien * (phi_c / 100))
        sau_phi_cty = clean_money(so_tien - phi_tra_cty)
        thuc_chuyen = parse_float_input(
            request.form.get('thuc_chuyen'),
            current_row['Thực chuyển'],
        )
        lai_lo = clean_money(tien_phi_khach - phi_tra_cty)
        dt_obj = datetime.strptime(tx_date, '%Y-%m-%d')
        compute_done_at = time.perf_counter()
        update_transaction_by_id(
            idx,
            {
                'transaction_date': tx_date,
                'month_name': dt_obj.strftime('%b'),
                'day_of_month': dt_obj.day,
                'owner_name': request.form.get('ten_chu_pos', '').strip(),
                'company_name': request.form.get('ten_cong_ty', '').strip(),
                'pos_machine': request.form.get('may_pos', '').strip(),
                'batch_number': request.form.get('so_lo', '').strip(),
                'amount': so_tien,
                'customer_fee_amount': tien_phi_khach,
                'amount_after_customer_fee': thanh_tien_sau_phi,
                'company_fee_amount': phi_tra_cty,
                'amount_after_company_fee': sau_phi_cty,
                'transferred_amount': thuc_chuyen,
                'profit_loss': lai_lo,
                'customer_fee_percent': f"{phi_k}%",
                'company_fee_percent': f"{phi_c}%",
                'note': request.form.get('ghi_chu', '').strip(),
            },
        )
        write_done_at = time.perf_counter()
    else:
        compute_done_at = load_done_at
        write_done_at = load_done_at
    response = redirect(url_for(
        'all_transactions',
        from_date=request.form.get('ret_from_date', ''),
        to_date=request.form.get('ret_to_date', ''),
        chu_pos=request.form.get('ret_chu', 'Tất cả'),
        cong_ty=request.form.get('ret_cty', 'Tất cả'),
        may_pos=request.form.get('ret_may', 'Tất cả')
    ))
    redirect_done_at = time.perf_counter()
    log_route_breakdown(
        'update_transaction_inline',
        load_ms=(load_done_at - started_at) * 1000,
        compute_ms=(compute_done_at - load_done_at) * 1000,
        write_ms=(write_done_at - compute_done_at) * 1000,
        redirect_ms=(redirect_done_at - write_done_at) * 1000,
    )
    return response

@app.route('/delete_transaction/<int:idx>')
def delete_transaction(idx):
    started_at = time.perf_counter()
    delete_transaction_by_id(idx)
    write_done_at = time.perf_counter()
    response = redirect(request.referrer)
    redirect_done_at = time.perf_counter()
    log_route_breakdown(
        'delete_transaction',
        write_ms=(write_done_at - started_at) * 1000,
        redirect_ms=(redirect_done_at - write_done_at) * 1000,
    )
    return response

@app.route('/delete_chu_pos/<name>')
def delete_chu_pos(name):
    delete_directory_by_owner(name)
    return redirect(url_for('settings'))

@app.route('/delete_may_pos/<name>')
def delete_may_pos(name):
    delete_directory_by_machine(name)
    return redirect(url_for('settings'))

@app.route('/delete_company/<name>')
def delete_company(name):
    delete_directory_by_company(name)
    return redirect(url_for('settings'))

@app.route('/save_note', methods=['POST'])
def save_note():
    started_at = time.perf_counter()
    key, text = request.form['key'], request.form['note'].strip()
    upsert_note(key, text)
    write_done_at = time.perf_counter()
    response = redirect(request.referrer)
    redirect_done_at = time.perf_counter()
    log_route_breakdown(
        'save_note',
        write_ms=(write_done_at - started_at) * 1000,
        redirect_ms=(redirect_done_at - write_done_at) * 1000,
    )
    return response

@app.route('/add_transfer', methods=['POST'])
def add_transfer():
    started_at = time.perf_counter()
    key = request.form['key']
    amount = float(request.form['amount_to_add'].replace(',', '') or 0)
    operation = request.form.get('operation', 'add')
    if amount > 0:
        add_transfer_to_transaction(key, amount, operation)
    write_done_at = time.perf_counter()
    response = redirect(request.referrer)
    redirect_done_at = time.perf_counter()
    log_route_breakdown(
        'add_transfer',
        write_ms=(write_done_at - started_at) * 1000,
        redirect_ms=(redirect_done_at - write_done_at) * 1000,
    )
    return response

@app.route('/add_cty_pay_quick', methods=['POST'])
def add_cty_pay_quick():
    started_at = time.perf_counter()
    key, amount = request.form['key'], float(request.form['amount'].replace(',', '') or 0)
    if amount > 0:
        body = key[4:] if key.startswith('CTY-') else key
        if len(body) > 11 and body[-11] == '-':
            company_name = body[:-11]
            transfer_date = body[-10:]
            add_company_transfer_entry(transfer_date, company_name, amount, 0)
    write_done_at = time.perf_counter()
    response = redirect(request.referrer)
    redirect_done_at = time.perf_counter()
    log_route_breakdown(
        'add_cty_pay_quick',
        write_ms=(write_done_at - started_at) * 1000,
        redirect_ms=(redirect_done_at - write_done_at) * 1000,
    )
    return response

@app.route('/delete_company_transfer/<int:transfer_id>')
def delete_company_transfer(transfer_id):
    started_at = time.perf_counter()
    delete_company_transfer_by_id(transfer_id)
    write_done_at = time.perf_counter()
    response = redirect(request.referrer or url_for('company_transfers'))
    redirect_done_at = time.perf_counter()
    log_route_breakdown(
        'delete_company_transfer',
        write_ms=(write_done_at - started_at) * 1000,
        redirect_ms=(redirect_done_at - write_done_at) * 1000,
    )
    return response

@app.route('/all_transactions')
def all_transactions():
    started_at = time.perf_counter()
    f_from_date, f_to_date = resolve_date_range(request.args)
    f_chu = 'Tất cả'
    f_cty, f_may = request.args.get('cong_ty', 'Tất cả'), request.args.get('may_pos', 'Tất cả')
    edit_idx = request.args.get('edit_idx', type=int)
    context = get_all_transactions_context(f_from_date, f_to_date, f_chu, f_cty, f_may)
    visible_owner_count = len({
        row['Tên Chủ POS']
        for row in context['transactions']
        if row.get('Tên Chủ POS')
    })
    context_done_at = time.perf_counter()
    response = render_template(
        'all_transactions.html',
        transactions=context['transactions'],
        summary=context['summary'],
        company_transfer_summary=context['company_transfer_summary'],
        owner_summary=context['owner_summary'],
        list_chu=context['list_chu'],
        list_cty=context['list_cty'],
        list_may=context['list_may'],
        owner_map=context['owner_map'],
        company_map=context['company_map'],
        sel_date=f_from_date if f_from_date == f_to_date else '',
        sel_from_date=f_from_date,
        sel_to_date=f_to_date,
        sel_chu=f_chu,
        sel_cty=f_cty,
        sel_may=f_may,
        show_owner_column=visible_owner_count > 1,
        edit_idx=edit_idx
    )
    render_done_at = time.perf_counter()
    log_route_breakdown(
        'all_transactions',
        context_ms=(context_done_at - started_at) * 1000,
        render_ms=(render_done_at - context_done_at) * 1000,
    )
    return response

if __name__ == '__main__':
    init_database()
    app.run(
        host=os.environ.get('APP_HOST', '0.0.0.0'),
        port=int(os.environ.get('APP_PORT', '5001')),
        debug=read_bool_env('APP_DEBUG', default=True),
    )
