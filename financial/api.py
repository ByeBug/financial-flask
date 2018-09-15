import re, datetime

from flask import Blueprint, g ,url_for, request, jsonify
from financial.db import get_db

bp = Blueprint('api', __name__, url_prefix='/api1')

@bp.route('/companylist')
def company_list():
    limit = int(request.args.get('limit', 100))
    offset = int(request.args.get('page', 0)) * limit

    cursor = get_db().cursor()

    result = {
        'error': '',
        'list': ''
    }

    c_list = []
    sql = """SELECT `c_id`, `name` FROM `c_client` 
        LIMIT %s OFFSET %s"""
    cursor.execute(sql, (limit, offset))
    q = cursor.fetchall()
    for i in q:
        c_list.append({
            'c_id': i[0],
            'name': i[1]
        })

    result['list'] = c_list

    cursor.close()

    return jsonify(result)


@bp.route('/search_company')
def search_company():
    keyword = request.args.get('keyword')
    industry = request.args.get('industry')
    limit = int(request.args.get('limit', 100))
    offset = int(request.args.get('page', 0)) * limit

    result = {
        'error': '',
        'count': '',
        'result': []
    }

    if keyword:
        cursor = get_db().cursor()

        # 从数据库中取得带有关键字的列表
        name_pattern = '%' + '%'.join(keyword) + '%'
        sql_result = []

        if industry:
            indus_pattern = '%' + industry + '%'

            sql = """SELECT COUNT(*) FROM `c_client`
                WHERE `name` LIKE %s
                AND `sfc` LIKE %s"""
            cursor.execute(sql, (name_pattern, indus_pattern))
            count = cursor.fetchone()[0]

            sql = """SELECT `c_id`, `name` FROM c_client
                WHERE `name` LIKE %s
                AND `sfc` LIKE %s"""
            cursor.execute(sql, (name_pattern, indus_pattern))
            q = cursor.fetchall()
            for i in q:
                sql_result.append(i)
        else:
            sql = """SELECT COUNT(*) FROM `c_client`
                WHERE `name` LIKE %s"""
            cursor.execute(sql, (name_pattern,))
            count = cursor.fetchone()[0]

            sql = """SELECT `c_id`, `name` FROM c_client
                WHERE `name` LIKE %s"""
            cursor.execute(sql, (name_pattern,))
            q = cursor.fetchall()
            for i in q:
                sql_result.append(i)

        cursor.close()

        # 对列表进行排序
        sort_result = []
        re_pattern = '.*?'.join(keyword)
        regex = re.compile(re_pattern)
        for i in sql_result:
            match = regex.search(i[1])
            if match:
                sort_result.append((len(match.group()), match.start(), i))
        sort_result = [i[2] for i in sorted(sort_result)]

        result['count'] = count
        result['result'] = sort_result[offset:offset+limit]
    else:
        result['error'] = 'Need keyword'

    return jsonify(result)


@bp.route('/search_industry')
def search_industry():
    industry = request.args.get('industry')
    limit = int(request.args.get('limit', 100))
    offset = int(request.args.get('page', 0)) * limit

    result = {
        'error': '',
        'count': '',
        'result': []
    }

    if industry:
        cursor = get_db().cursor()

        # 从数据库中取得带有关键字的列表
        sql_pattern = '%' + industry + '%'

        sql = """SELECT COUNT(*) FROM c_client
            WHERE `sfc` LIKE %s"""
        cursor.execute(sql, (sql_pattern,))
        count = cursor.fetchone()[0]

        sql = """SELECT `c_id`, `name` FROM c_client
            WHERE `sfc` LIKE %s
            LIMIT %s OFFSET %s"""
        cursor.execute(sql, (sql_pattern, limit, offset))

        result['count'] = count
        result['result'] = cursor.fetchall()

        cursor.close()
    else:
        result['error'] = 'Need industry'

    return jsonify(result)


@bp.route('/baseinfo')
def baseinfo():
    c_id = request.args.get('c_id')

    result = {
        'error': '',
        'name': '',
        's_name': '',
        'estab_date': '',
        'money': '',
        'legal_name': '',
        'reg_address': '',
        'logo': '',
        'main_business': '',  # 暂无
        'top_world': '',      # 暂无
        'top_china': '',      # 暂无
        'list_info': ''
    }

    if c_id:
        cursor = get_db().cursor()

        sql = """
        select `name`, a_name, estab_time, money, legal_name, reg_address, logo
        from c_client
        where c_id = %s"""
        cursor.execute(sql, (c_id, ))
        q = cursor.fetchone()
        if q:
            result['name'] = q[0]
            result['s_name'] = q[1]
            result['estab_date'] = q[2]
            result['money'] = q[3]
            result['legal_name'] = q[4]
            result['reg_address'] = q[5]
            if q[6]:
                result['logo'] = q[6]
            else:
                result['logo'] = '/logo/默认图片.jpg'

            list_info = []
            sql = """
            select a_code, b_code, h_code, x_code, n_code, nas_code 
            from c_client where c_id = %s"""
            cursor.execute(sql, (c_id, ))
            q = cursor.fetchone()
            codes = []
            for i in q:
                if i and re.match(r'\d{6}', i):
                        codes.append(i)
            if codes:
                sql = """
                select s_name, s_id, list_date, address
                from shares
                where s_id in %s"""
                cursor.execute(sql, (codes, ))
                for i in cursor.fetchall():
                    list_info.append({
                        's_name': i[0],
                        's_id': i[1],
                        'date': i[2],
                        'address': i[3]
                    })
            result['list_info'] = list_info

        else:
            result['error'] = 'No such firm'
        
        cursor.close()
    else:
        result['error'] = 'Need c_id'

    return jsonify(result)

@bp.route('/holders')
def holders():
    c_id = request.args.get('c_id')

    result = {
        'error': '',
        'name': '',
        'type': '',
        'holder': {},
        'act_control': '',
        'holders': []
    }
    holders = []

    if c_id:
        cursor = get_db().cursor()
        
        sql = """
        select name, enterprise_type, act_contr_id, is_listed from c_client
        where c_id = %s
        """
        cursor.execute(sql, (c_id, ))
        q = cursor.fetchone()
        if q:
            result['name'] = q[0]
            result['type'] = q[1]
            result['act_control'] = q[2]
            is_listed = q[3]
            if is_listed:
                # 上市公司
                sql = """
                SELECT shaholder_name, rate FROM c_top_shaholder
                WHERE c_id = %s
                    AND deadline = (
                        SELECT MAX(deadline) FROM c_top_shaholder
                        WHERE c_id = %s
                    )
                ORDER BY CONVERT(rate,DECIMAL(5,2)) desc
                """
                cursor.execute(sql, (c_id, c_id))
                for i in cursor.fetchall():
                    holders.append({'name': i[0], 'rate': i[1]})
            else:
                # 非上市公司
                sql = """
                select shaholder_name, invest_rate from c_shaholder
                where c_id = %s
                order by convert(invest_rate, decimal(5,2)) desc
                """
                cursor.execute(sql, (c_id, ))
                for i in cursor.fetchall():
                    holders.append({'name': i[0], 'rate': i[1]})
            
            result['holders'] = holders
            result['holder'] = holders[0] if holders else ''

        else:
            result['error'] = 'No such firm'
        
        cursor.close()
    else:
        result['error'] = 'need c_id'

    return jsonify(result)


@bp.route('/firmgraph_holders')
def firmgraph_holders():
    def get_holders(c_id, cursor):
        holders = []

        sql = """select is_listed from c_client where c_id = %s"""
        cursor.execute(sql, (c_id,))
        q = cursor.fetchone()
        if q:
            if q[0]:
                # 上市公司
                sql = """select shaholder_name, `rate` from c_top_shaholder where c_id = %s
                AND deadline = (SELECT MAX(deadline) FROM c_top_shaholder WHERE c_id = %s)
                order by convert(rate, decimal(5,2)) desc"""
                cursor.execute(sql, (c_id, c_id))
                q = cursor.fetchall()
                for i in q:
                    holder = {
                        'code': '',
                        'name': i[0],
                        'rate': i[1]
                    }
                    sql = """select c_id from c_client where `name` = %s"""
                    cursor.execute(sql, (i[0]))
                    temp_q = cursor.fetchone()
                    if temp_q:
                        holder['code'] = temp_q[0]
                    
                    holders.append(holder)
                
                return holders
            else:
                # 非上市公司
                sql = """select shaholder_id, shaholder_name, invest_rate as rate 
                from c_shaholder where c_id = %s
                order by convert(rate, decimal(5,2)) desc"""
                cursor.execute(sql, (c_id, ))
                q = cursor.fetchall()
                for i in q:
                    holder = {
                        'code': i[0],
                        'name': i[1],
                        'rate': i[2]
                    }
                    holders.append(holder)
                
                return holders
        else:
            return []


    c_id = request.args.get('c_id')
    
    result = {
        'error': '',
        'holders': []
    }

    if c_id:
        cursor = get_db().cursor()

        holders = get_holders(c_id, cursor)
        result['holders'] = holders
        
        cursor.close()
    else:
        result['error'] = 'Need c_id'

    return jsonify(result)
        

@bp.route('/firmgraph_investments')
def firmgraph_investments():
    def get_investments(c_id, cursor):
        investments = []

        sql_investments = """
        select i_id, `name`, rate from c_investment where c_id = %s
        """
        cursor.execute(sql_investments, (c_id, ))
        q = cursor.fetchall()
        for i in q:
            investment = {
                'code': i[0],
                'name': i[1],
                'rate': i[2]
            }

            investments.append(investment)

        return investments


    c_id = request.args.get('c_id')

    result = {
        'error': '',
        'investments': []
    }

    if c_id:
        cursor = get_db().cursor()

        investments = get_investments(c_id, cursor)
        result['investments'] = investments
        
        cursor.close()
    else:
        result['error'] = 'Need c_id'

    return jsonify(result)


@bp.route('/managers')
def managers():
    c_id = request.args.get('c_id')

    result = {
        'error': '',
        'managers': []
    }
    managers = []

    if c_id:
        cursor = get_db().cursor()

        sql = """
        select c.e_name, c.post, e.abstract
        from client_executive as c
        inner join e_executive as e
        on (c.c_id = e.c_id and c.e_name = e.e_name)
        where c.c_id = %s"""

        # sql = """SELECT e_name, post, abstract
        # FROM e_executive
        # WHERE c_id = %s"""
        cursor.execute(sql, (c_id, ))
        for i in cursor.fetchall():
            managers.append({
                'name': i[0],
                'post': i[1],
                'abstract': i[2]
            })
        
        result['managers'] = managers

        cursor.close()
    else:
        result['error'] = 'Need c_id'

    return jsonify(result)


@bp.route('/changeinfo')
def changeinfo():
    c_id = request.args.get('c_id')

    result = {
        'error': '',
        'changeinfos': []
    }
    changeinfos = []

    if c_id:
        cursor = get_db().cursor()

        sql = """
        select `time`, `item`, `before`, `after`
        from c_changeinfo where c_id=%s"""
        cursor.execute(sql, (c_id, ))
        for i in cursor.fetchall():
            changeinfos.append({
                'time': str(i[0]),
                'item': i[1],
                'before': i[2],
                'after': i[3],
            })
        
        result['changeinfos'] = changeinfos

        cursor.close()
    else:
        result['error'] = 'Need c_id'

    return jsonify(result)


@bp.route('/business')
def business():
    c_id = request.args.get('c_id')

    result = {
        'error': '',
        'year': '',
        'operate_rev': '',
        'operate_rev_YOY': '',
        'profit': '',
        'profit_YOY': '',
        'key_business_3_year': [],
        'key_business_last': []
    }

    key_business_3_year = []
    key_business_last = []
    if c_id:
        cursor = get_db().cursor()

        # XX年，营业额，同比，利润总额，同比
        sql = """
        select report_date, tot_operate_rev, sum_profit from fin_income
        where c_id = %s and report_type = 1
        order by report_date desc
        limit 2
        """
        cursor.execute(sql, (c_id, ))
        q = cursor.fetchall()
        if q:
            result['year'] = q[0][0].year
            result['operate_rev'] = q[0][1]
            result['profit'] = q[0][2]
        if len(q) > 1:
            operate_rev_YOY = (int(q[0][1]) - int(q[1][1])) / int(q[1][1])
            profit_YOY = (int(q[0][2]) - int(q[1][2])) / int(q[1][2])
            result['operate_rev_YOY'] = format(operate_rev_YOY, '0.2%')
            result['profit_YOY'] = format(profit_YOY, '0.2%')
        # 获取最近三年的时间
        sql = """
        select DISTINCT deadline
        from c_key_business
        where c_id = %s and MONTH(deadline)=12
        order by deadline desc
        limit 3
        """
        cursor.execute(sql, (c_id, ))
        dates = cursor.fetchall()
        if dates:
            sql = """
            select deadline, classify, type, income, inc_rate, cost, cost_rate, profit, pro_rate, mon_rate
            from c_key_business
            where c_id = %s and deadline = %s
            """
            for i in dates:
                cursor.execute(sql, (c_id, i))
                key_business_3_year.append({
                    'date': i[0],
                    'data': cursor.fetchall()
                })
        # 最近一期
        sql = """
        select deadline, classify, type, income, inc_rate, cost, cost_rate, profit, pro_rate, mon_rate
        from c_key_business
        where c_id = %s
        and deadline = (select max(deadline) from c_key_business where c_id = %s)
        """
        cursor.execute(sql, (c_id, c_id))
        key_business_last = cursor.fetchall()

        result['key_business_3_year'] = key_business_3_year
        result['key_business_last'] = key_business_last

        cursor.close()
    else:
        result['error'] = 'Need c_id'

    return jsonify(result)

@bp.route('/financialstatement')
def financial_statement():
    c_id = request.args.get('c_id')

    result = {
        'error': '',
        'year': '',
        'sum_asset': '',
        'sum_debt': '',
        'sum_owners_equity': '',
        'asset_debt_ratio': '',
        'operate_rev': '',
        'operate_rev_YOY': '',
        'net_profit': '',
        'net_profit_YOY': '',
        'statement_2_year': [],
        'statement_last': {}
    }

    statement_2_year = []
    statement_last = {}
    if c_id:
        cursor = get_db().cursor()

        # 最近两年
        sql = """
        select report_date, name, accounting_office, sum_ass, sum_liab, sum_she_equity, operate_rev, 
            net_profit, sum_curr_ass, monetary_fund, account_rec, other_rec, advance_pay, inventory, fixed_ass, 
            sum_curr_liab, st_borrow, bill_pay, account_pay, advance_rec, other_pay, lt_borrow, sum_parent_equity,
            operate_exp, operate_tax, operate_profig, sum_profit, parent_net_profit, net_operate_cash_flow, 
            net_inv_cash_flow, net_fin_cash_flow
        from statement_view
        where c_id = %s and report_type = 1
        order by report_date desc limit 3
        """ 
        cursor.execute(sql, (c_id, ))
        q = cursor.fetchall()
        # 有的财务数据可能缺少某些项
        if q:
            result['year'] = q[0][0].year
            result['sum_asset'] = q[0][3]
            result['sum_debt'] = q[0][4]
            result['sum_owners_equity'] = q[0][5]
            result['asset_debt_ratio'] = format(int(q[0][4]) / int(q[0][3]), '0.2%')
            result['operate_rev'] = q[0][6]
            result['net_profit'] = q[0][7]
            if len(q) > 1:
                try:
                    operate_rev_YOY = (int(q[0][6]) - int(q[1][6])) / int(q[1][6])
                    result['operate_rev_YOY'] = format(operate_rev_YOY, '0.2%')
                except:
                    pass
                try:
                    net_profit_YOY = (int(q[0][7]) - int(q[1][7])) / int(q[1][7])
                    result['net_profit_YOY'] = format(net_profit_YOY, '0.2%')
                except:
                    pass
            for index in range((len(q) - 1) or 1):
                i = q[index]

                try:
                    net_cash_flow = int(i[28]) + int(i[29]) + int(i[30])
                except:
                    net_cash_flow = ''
                try:
                    asset_debt_ratio = format(int(i[4]) / int(i[3]), '0.2%')
                except:
                    asset_debt_ratio = ''
                try:
                    current_ratio = format(int(i[8]) / int(i[15]), '0.2%')
                except:
                    current_ratio = ''
                try:
                    quick_ratio = format((int(i[8]) - int(i[13]) - int(i[12])) / int(i[15]), '0.2%')
                except:
                    quick_ratio = ''
                try:
                    total_asset_turnover = format(int(i[6]) / int(i[3]), '0.2%')
                except:
                    total_asset_turnover = ''
                try:
                    main_business_profit_ratio = format(int(i[25]) / int(i[6]), '0.2%')
                except:
                    main_business_profit_ratio = ''
                try:
                    net_asset_return_ratio = format(int(i[27]) / (int(i[3]) - int(i[4])), '0.2%')
                except:
                    net_asset_return_ratio = ''
                
                j = q[index + 1]
                try:
                    account_rec_turnover = format(int(i[6]) / ((int(i[10]) + int(j[10])) / 2), '0.2f')
                except:
                    account_rec_turnover = ''
                try:
                    inventory_turnover = format(int(i[23]) / ((int(i[13]) + int(j[13])) / 2), '0.2f')
                except:
                    inventory_turnover = ''
                try:
                    total_asset_return_ratio = format(int(i[26]) / ((int(i[3]) + int(j[3])) / 2), '0.2%')
                except:
                    total_asset_return_ratio = ''
                try:
                    business_growth_rate = format((int(i[6]) - int(j[6])) / int(j[6]), '0.2%')
                except:
                    business_growth_rate = ''
                try:
                    total_asset_growth_rate = format((int(i[3]) - int(j[3])) / int(j[3]), '0.2%')
                except:
                    total_asset_growth_rate = ''
                try:
                    net_profit_growth_rate = format((int(i[7]) - int(j[7])) / int(j[7]), '0.2%')
                except:
                    net_profit_growth_rate = ''

                
                item = {
                    'date': str(i[0]),
                    'sum_asset': i[3],
                    'curr_asset': i[8],
                    'monetary_fund': i[9],
                    'account_rec': i[10],
                    'other_rec': i[11],
                    'advance_pay': i[12],
                    'inventory': i[13],
                    'fixed_ass': i[14],
                    'sum_liab': i[4],
                    'sum_curr_liab': i[15],
                    'st_borrow': i[16],
                    'bill_pay': i[17],
                    'account_pay': i[18],
                    'advance_rec': i[19],
                    'other_pay': i[20],
                    'lt_borrow': i[21],
                    'sum_she_equity': i[5],
                    'sum_parent_equity': i[22],
                    'operate_rev': i[6],
                    'operate_exp': i[23],
                    'operate_tax': i[24],
                    'operate_profit': i[25],
                    'sum_profit': i[26],
                    'net_profit': i[7],
                    'parent_net_profit': i[27],
                    'net_operate_cash_flow': i[28],
                    'net_inv_cash_flow': i[29],
                    'net_fin_cash_flow': i[30],
                    'net_cash_flow': net_cash_flow,
                    'asset_debt_ratio': asset_debt_ratio,
                    'current_ratio': current_ratio,
                    'quick_ratio': quick_ratio,
                    'account_rec_turnover': account_rec_turnover,
                    'inventory_turnover': inventory_turnover,
                    'total_asset_turnover': total_asset_turnover,
                    'main_business_profit_ratio': main_business_profit_ratio,
                    'net_asset_return_ratio': net_asset_return_ratio,
                    'total_asset_return_ratio': total_asset_return_ratio,
                    'business_growth_rate': business_growth_rate,
                    'total_asset_growth_rate': total_asset_growth_rate,
                    'net_profit_growth_rate': net_profit_growth_rate,
                }
                statement_2_year.append(item)

        # 最近一期
        sql = """
        select report_date, name, accounting_office, sum_ass, sum_liab, sum_she_equity, operate_rev, 
            net_profit, sum_curr_ass, monetary_fund, account_rec, other_rec, advance_pay, inventory, fixed_ass,
            sum_curr_liab, st_borrow, bill_pay, account_pay, advance_rec, other_pay, lt_borrow, sum_parent_equity,
            operate_exp, operate_tax, operate_profig, sum_profit, parent_net_profit, net_operate_cash_flow, 
            net_inv_cash_flow, net_fin_cash_flow
        from statement_view
        where c_id = %s and report_type = 0
        and report_date = (select max(report_date) from statement_view where c_id = %s )
        """
        cursor.execute(sql, (c_id, c_id))
        q = cursor.fetchone()
        if q:
            try:
                net_cash_flow = int(q[28]) + int(q[29]) + int(q[30])
            except:
                net_cash_flow = ''
            try:
                asset_debt_ratio = format(int(q[4]) / int(q[3]), '0.2%')
            except:
                asset_debt_ratio = ''
            try:
                current_ratio = format(int(q[8]) / int(q[15]), '0.2%')
            except:
                current_ratio = ''
            try:
                quick_ratio = format((int(q[8]) - int(q[13]) - int(q[12])) / int(q[15]), '0.2%')
            except:
                quick_ratio = ''
            account_rec_turnover = ''
            inventory_turnover = ''
            try:
                total_asset_turnover = format(int(q[6]) / int(q[3]), '0.2%')
            except:
                total_asset_turnover = ''
            try:
                main_business_profit_ratio = format(int(q[25]) / int(q[6]), '0.2%')
            except:
                main_business_profit_ratio = ''
            net_asset_return_ratio = ''
            total_asset_return_ratio = ''
            business_growth_rate = ''
            total_asset_growth_rate = ''
            net_profit_growth_rate = ''
            statement_last = {
                'date': str(q[0]),
                'sum_asset': q[3],
                'curr_asset': q[8],
                'monetary_fund': q[9],
                'account_rec': q[10],
                'other_rec': q[11],
                'advance_pay': q[12],
                'inventory': q[13],
                'fixed_ass': q[14],
                'sum_liab': q[4],
                'sum_curr_liab': q[15],
                'st_borrow': q[16],
                'bill_pay': q[17],
                'account_pay': q[18],
                'advance_rec': q[19],
                'other_pay': q[20],
                'lt_borrow': q[21],
                'sum_she_equity': q[5],
                'sum_parent_equity': q[22],
                'operate_rev': q[6],
                'operate_exp': q[23],
                'operate_tax': q[24],
                'operate_profit': q[25],
                'sum_profit': q[26],
                'net_profit': q[7],
                'parent_net_profit': q[27],
                'net_operate_cash_flow': q[28],
                'net_inv_cash_flow': q[29],
                'net_fin_cash_flow': q[30],
                'net_cash_flow': net_cash_flow,
                'asset_debt_ratio': asset_debt_ratio,
                'current_ratio': current_ratio,
                'quick_ratio': quick_ratio,
                'account_rec_turnover': account_rec_turnover,
                'inventory_turnover': inventory_turnover,
                'total_asset_turnover': total_asset_turnover,
                'main_business_profit_ratio': main_business_profit_ratio,
                'net_asset_return_ratio': net_asset_return_ratio,
                'total_asset_return_ratio': total_asset_return_ratio,
                'business_growth_rate': business_growth_rate,
                'total_asset_growth_rate': total_asset_growth_rate,
                'net_profit_growth_rate': net_profit_growth_rate
            }
        
        result['statement_2_year'] = statement_2_year
        result['statement_last'] = statement_last

        cursor.close()
    else:
        result['error'] = 'Need c_id'

    return jsonify(result)

@bp.route('/financinginfo')
def financing_info():
    c_id = request.args.get('c_id')
    cursor = get_db().cursor()

    result = {
        'error': '',
        'g_id': '',
        'credit_detail': '',
        'credit_amount': '',
        'organ': '',
        'date': '',
        'rating': '',
        'move': '',
        'bonds': '',
        'debt_info_3_year': '',
        'debt_info_last': ''
    }

    credit_detail = []
    credit_amount = []
    bonds = []
    debt_info_3_year = []
    debt_info_last = []
    if c_id:
        # 授信信息
        sql = """select time, bank, currency, amount, used, unused
        from c_credit_2 where c_id = %s"""
        cursor.execute(sql, c_id)
        q = cursor.fetchall()
        for i in q:
            credit_detail.append({
                'time': str(i[0]),
                'bank': i[1],
                'currency': i[2],
                'amount': i[3],
                'used': i[4],
                'unused': i[5]
            })
        # 授信信息合计
        sql = """select currency, sum(amount), sum(used), sum(unused)
        from c_credit_2 where c_id = %s
        GROUP BY currency"""
        cursor.execute(sql, c_id)
        q = cursor.fetchall()
        for i in q:
            credit_amount.append({
                'currency': i[0],
                'amount': i[1],
                'used': i[2],
                'unused': i[3]
            })
        # 债券信息
        name = ''
        sql = "select name, group_id from c_client where c_id = %s"
        cursor.execute(sql, (c_id, ))
        q = cursor.fetchone()
        if q:
            name = '%' + q[0] + '%'
            result['g_id'] = q[1]
        sql = """
        select debt_subject, s_id, s_name, s_short_name, init_face_value, total, rest, inter_type, 
            inter_rate, deadline, list_date, list_address, classify, lead_underwriter
        from b_bond where date(honour_date) > curdate() and
        debt_subject like %s
        order by list_date desc"""
        cursor.execute(sql, (name, ))
        q = cursor.fetchall()
        if q:
            s_ids = []
            for i in q:
                item = {
                    'debt_subject': i[0],
                    's_id': i[1],
                    's_name': i[2],
                    's_short_name': i[3],
                    'init_face_value': i[4],
                    'total': i[5],
                    'rest': i[6],
                    'inter_type': i[7],
                    'inter_rate': i[8],
                    'deadline': i[9],
                    'list_date': str(i[10]),
                    'list_address': i[11],
                    'classify': i[12],
                    'lead_underwriter': i[13]
                }
                bonds.append(item)
                s_ids.append(i[1])
            
            # 主体评级
            sql = """
            select organ, date(issuer_time), issuer_rating, issuer_move 
            from b_bond_rating where s_id in %s
            order by date(issuer_time) desc"""
            cursor.execute(sql, (s_ids, ))
            q = cursor.fetchone()
            result['organ'] = q[0]
            result['date'] = str(q[1])
            result['rating'] = q[2]
            result['move'] = q[3]

        # 最近三年有息负债情况
        sql = """
        select report_date, st_borrow, bill_pay, non_curr_liab_one_year, lt_borrow, bond_pay, 
            leasehold, loan_rec, issue_bond_rec, repay_debt_pay, dividend_profit_or_interest_pay, 
            net_operate_cash_flow, net_inv_cash_flow, net_fin_cash_flow
        from debt_info
        where c_id = %s and report_type = 1
        order by report_date DESC
        limit 3
        """
        cursor.execute(sql, (c_id, ))
        debt_info_3_year = cursor.fetchall()
        # 最新一期
        sql = """
        select report_date, st_borrow, bill_pay, non_curr_liab_one_year, lt_borrow, bond_pay, 
            leasehold, loan_rec, issue_bond_rec, repay_debt_pay, dividend_profit_or_interest_pay, 
            net_operate_cash_flow, net_inv_cash_flow, net_fin_cash_flow
        from debt_info
        where c_id = %s and report_type = 0
        and report_date = (select max(report_date) from debt_info where c_id=%s)
        """
        cursor.execute(sql, (c_id, c_id))
        debt_info_last = cursor.fetchone() or []

        result['credit_detail'] = credit_detail
        result['credit_amount'] = credit_amount
        result['bonds'] = bonds
        result['debt_info_3_year'] = debt_info_3_year
        result['debt_info_last'] = debt_info_last
    else:
        result['error'] = 'Need c_id'

    cursor.close()

    return jsonify(result)


@bp.route('/financing_group_info')
def financing_group_info():
    g_id = request.args.get('g_id')
    cursor = get_db().cursor()

    # 有授信信息的公司
    sql = """select distinct c_id from c_credit_2"""
    cursor.execute(sql)
    q = cursor.fetchall()
    companies_with_credit = set([i[0] for i in q])
    # 有债券信息的公司
    sql = """select distinct debt_subject from b_bond"""
    cursor.execute(sql)
    q = cursor.fetchall()
    companies_with_bonds = set([i[0] for i in q])
    # 有负债信息的公司
    sql = """select distinct c_id from debt_info"""
    cursor.execute(sql)
    q = cursor.fetchall()
    companies_with_debts = set([i[0] for i in q])

    result = {
        'error': '',
        'credit': '',
        'bonds': '',
        'debts': '',
    }

    credit = []
    bonds = []
    debts = []
    if g_id:
        # 集团内企业
        sql = 'select c_id, name from c_client where group_id=%s'
        if cursor.execute(sql, g_id):
            q = cursor.fetchall()
            for i in q:
                # 该企业授信
                if i[0] in companies_with_credit:
                    # 授信信息
                    sql = """select time, bank, currency, amount, used, unused
                    from c_credit_2 where c_id = %s"""
                    cursor.execute(sql, i[0])
                    res = cursor.fetchall()
                    credit_detail = []
                    for r in res:
                        credit_detail.append({
                            'time': str(r[0]),
                            'bank': r[1],
                            'currency': r[2],
                            'amount': r[3],
                            'used': r[4],
                            'unused': r[5]
                        })
                    credit.append({
                        'name': i[1],
                        'credit_detail': credit_detail
                    })
                
                # 该企业债券
                if i[1] in companies_with_bonds:
                    name = '%' + i[1] + '%'
                    sql = """
                    select debt_subject, s_id, s_name, s_short_name, init_face_value, total, rest, inter_type, 
                        inter_rate, deadline, list_date, list_address, classify, lead_underwriter
                    from b_bond where date(honour_date) > curdate() and
                    debt_subject like %s"""
                    cursor.execute(sql, (name, ))
                    res = cursor.fetchall()
                    if res:
                        for r in res:
                            item = {
                                'debt_subject': r[0],
                                's_id': r[1],
                                's_name': r[2],
                                's_short_name': r[3],
                                'init_face_value': r[4],
                                'total': r[5],
                                'rest': r[6],
                                'inter_type': r[7],
                                'inter_rate': r[8],
                                'deadline': r[9],
                                'list_date': str(r[10]),
                                'list_address': r[11],
                                'classify': r[12],
                                'lead_underwriter': r[13]
                            }
                            bonds.append(item)
                # 该企业负债情况
                if i[0] in companies_with_debts:
                    # 最新一期
                    sql = """
                    select report_date, st_borrow, bill_pay, non_curr_liab_one_year, lt_borrow, bond_pay, 
                        leasehold, loan_rec, issue_bond_rec, repay_debt_pay, dividend_profit_or_interest_pay, 
                        net_operate_cash_flow, net_inv_cash_flow, net_fin_cash_flow
                    from debt_info
                    where c_id = %s and report_type = 0
                    and report_date = (select max(report_date) from debt_info where c_id=%s)
                    """
                    cursor.execute(sql, (i[0], i[0]))
                    debt_info_last = cursor.fetchone() or []
                    # 最近三年有息负债情况
                    sql = """
                    select report_date, st_borrow, bill_pay, non_curr_liab_one_year, lt_borrow, bond_pay, 
                        leasehold, loan_rec, issue_bond_rec, repay_debt_pay, dividend_profit_or_interest_pay, 
                        net_operate_cash_flow, net_inv_cash_flow, net_fin_cash_flow
                    from debt_info
                    where c_id = %s and report_type = 1
                    order by report_date DESC
                    limit 3
                    """
                    cursor.execute(sql, (i[0], ))
                    debt_info_3_year = cursor.fetchall()

                    debt = {
                        'name': i[1],
                        'debt_info_3_year': debt_info_3_year,
                        'debt_info_last': debt_info_last
                    }
                    debts.append(debt)
        
        result['credit'] = credit
        result['bonds'] = bonds
        result['debts'] = debts
    else:
        result['error'] = 'Need g_id'

    cursor.close()

    return jsonify(result)


@bp.route('/financing_info_0729')
def financing_info_0729():
    c_id = request.args.get('c_id')

    result = {
        'error': '',
        'g_id': '',
        'credit_total': [],
        'credit_detail': [],
        'bond_total': '',
        'bond_detail': '',
        'rating': '',
        'debt': '',
        'share_financing_total': '',
        'share_financing_detail': ''
    }

    if c_id:
        cursor = get_db().cursor()

        # 授信统计信息
        credit_total = []
        sql = """select currency, sum(amount), sum(used), sum(unused)
        from c_credit_2 where c_id = %s
        group by currency"""
        cursor.execute(sql, c_id)
        q = cursor.fetchall()
        for i in q:
            credit_total.append({
                'currency': i[0],
                'amount': i[1],
                'used': i[2],
                'unused': i[3]
            })
        result['credit_total'] = credit_total
        # 授信详细信息
        credit_detail = []
        sql = """
        select bank, currency, amount, used, unused, limit_date
        from c_credit_2 where c_id = %s
        order by used desc"""
        cursor.execute(sql, c_id)
        q = cursor.fetchall()
        for i in q:
            credit_detail.append({
                'bank': i[0],
                'currency': i[1],
                'amount': i[2],
                'used': i[3],
                'unused': i[4],
                'limit_date': i[5]
            })
        result['credit_detail'] = credit_detail
        # 债券
        name = ''
        sql = "select name, group_id from c_client where c_id = %s"
        cursor.execute(sql, c_id)
        q = cursor.fetchone()
        if q:
            name = '%' + q[0] + '%'
            result['g_id'] = q[1]
        # 债券统计信息
        bond_total = {
            'rest': '',
            'total': '',
            'avg_deadline': '',
            'avg_inter_rate': '',
            'classify': []
        }
        sql = """select sum(rest), sum(total), avg(deadline), avg(inter_rate) 
        from b_bond where date(honour_date) > curdate() and
        debt_subject like %s"""
        cursor.execute(sql, name)
        q = cursor.fetchone()
        if q:
            bond_total['rest'] = q[0]
            bond_total['total'] = q[1]
            bond_total['avg_deadline'] = q[2]
            bond_total['avg_inter_rate'] = q[3]
        sql = """select classify, count(*), sum(rest), sum(total)
        from b_bond where date(honour_date) > curdate() and
        debt_subject like %s
        group by classify"""
        cursor.execute(sql, name)
        q = cursor.fetchall()
        for i in q:
            bond_total['classify'].append({
                'classify': i[0],
                'count': i[1],
                'rest': i[2],
                'total': i[3]
            })

        result['bond_total'] = bond_total

        # 债券详细信息
        bond_detail = []
        rating = {
            'organ': '',
            'date': '',
            'rating': '',
            'move': ''
        }

        sql = """
        select debt_subject, s_id, s_name, s_short_name, init_face_value, total, rest, inter_type, 
            inter_rate, deadline, list_date, list_address, classify, lead_underwriter
        from b_bond where date(honour_date) > curdate() and
        debt_subject like %s
        order by list_date desc"""
        cursor.execute(sql, (name, ))
        q = cursor.fetchall()
        if q:
            s_ids = []
            for i in q:
                item = {
                    'debt_subject': i[0],
                    's_id': i[1],
                    's_name': i[2],
                    's_short_name': i[3],
                    'init_face_value': i[4],
                    'total': i[5],
                    'rest': i[6],
                    'inter_type': i[7],
                    'inter_rate': i[8],
                    'deadline': i[9],
                    'list_date': str(i[10]),
                    'list_address': i[11],
                    'classify': i[12],
                    'lead_underwriter': i[13]
                }
                bond_detail.append(item)
                s_ids.append(i[1])
            
            # 主体评级
            sql = """
            select organ, date(issuer_time), issuer_rating, issuer_move 
            from b_bond_rating where s_id in %s
            order by date(issuer_time) desc
            limit 1"""
            cursor.execute(sql, (s_ids, ))
            q = cursor.fetchone()
            rating['organ'] = q[0]
            rating['date'] = str(q[1])
            rating['rating'] = q[2]
            rating['move'] = q[3]

        result['bond_detail'] = bond_detail
        result['rating'] = rating

        # 有息负债
        debt = {
            'total': '',
            '3_years': [],
            'last': []
        }
        # 最近一期
        sql = """select sum_liab, report_date, st_borrow, bill_pay, non_curr_liab_one_year, 
        lt_borrow, bond_pay, leasehold, debt_total, loan_rec, issue_bond_rec, 
        repay_debt_pay, dividend_profit_or_interest_pay, flow_total
        from debt_info where report_type = 0 and c_id = %s
        order by report_date desc
        limit 1"""
        cursor.execute(sql, c_id)  
        q = cursor.fetchone()      
        if q:
            q = list(q)
            debt['total'] = q[0]
            debt['last'] = q[1:]
            debt['last'][0] = str(debt['last'][0])
        # 近三年
        sql = """select report_date, st_borrow, bill_pay, non_curr_liab_one_year, 
        lt_borrow, bond_pay, leasehold, debt_total, loan_rec, issue_bond_rec, 
        repay_debt_pay, dividend_profit_or_interest_pay, flow_total
        from debt_info where report_type = 1 and c_id = %s
        order by report_date desc
        limit 3"""
        cursor.execute(sql, c_id)
        q = cursor.fetchall()
        for i in q:
            i = list(i)
            i[0] = str(i[0])
            debt['3_years'].append(i)

        result['debt'] = debt
        # 股权融资
        sql = """select a_code, b_code, h_code, x_code, n_code, nas_code 
        from c_client where c_id = %s"""
        cursor.execute(sql, c_id)
        q = cursor.fetchone() or []
        codes = []
        for i in q:
            if i and re.match(r'\d{6}', i):
                codes.append(i)
        share_financing_total = {
            'total': 0,
            'detail': ''
        }
        if codes:
            sql = """select issue_type, sum(issue_count*issue_price) from shares_financing
            where s_id in %s
            group by issue_type"""
            cursor.execute(sql, (codes,))
            q = cursor.fetchall()
            share_financing_total['detail'] = q
            for i in q:
                share_financing_total['total'] += i[1]
        
        result['share_financing_total'] = share_financing_total

        # 股权融资详细
        share_financing_detail = []
        if codes:
            sql = """select s_id, s_name, list_date, address, lead_underwriter
            from shares where s_id in %s"""
            cursor.execute(sql, (codes,))
            q = cursor.fetchall()
            for i in q:
                item = {
                    's_id': i[0],
                    's_name': i[1],
                    'list_date': i[2],
                    'address': i[3],
                    'lead_underwriter': i[4],
                    'detail': []
                }
                sql = """select issue_type, issue_date, issue_count, issue_price, raise_funds
                from shares_financing where s_id = %s"""
                cursor.execute(sql, i[0])
                tmp = cursor.fetchall()
                for tmp_i in tmp:
                    tmp_i = list(tmp_i)
                    tmp_i[1] = str(tmp_i[1])
                    item['detail'].append(tmp_i)

                share_financing_detail.append(item)

        result['share_financing_detail'] = share_financing_detail

        cursor.close()
    else:
        result['error'] = 'Need c_id'

    return jsonify(result)


@bp.route('/financing_group_info_0729')
def financing_group_info_0729():
    g_id = request.args.get('g_id')

    result = {
        'error': '',
        'credit_total': '',
        'credit_detail': '',
        'bond_total': '',
        'bond_detail': '',
        'rating': '',
        'debt': '',
        'share_financing_total': '',
        'share_financing_detail': ''
    }

    if g_id:
        cursor = get_db().cursor()

        sql = """select c_id, name from c_client where group_id = %s"""
        cursor.execute(sql, g_id)
        q = cursor.fetchall()
        # 集团内公司列表
        c_ids = []
        names = []
        for i in q:
            c_ids.append(i[0])
            names.append(i[1])
        
        if not c_ids:
            result['error'] = 'No company'

            return jsonify(result)
        
        # 授信
        # 授信统计信息
        credit_total = []
        sql = """select currency, sum(amount), sum(used), sum(unused)
        from c_credit_2 where c_id in %s
        group by currency"""
        cursor.execute(sql, (c_ids,))
        q = cursor.fetchall()
        for i in q:
            credit_total.append({
                'currency': i[0],
                'amount': i[1],
                'used': i[2],
                'unused': i[3]
            })
        
        result['credit_total'] = credit_total

        # 授信详细信息
        credit_detail = []
        sql = """
        select bank, currency, amount, used, unused, name, limit_date
        from c_credit_2 where c_id in %s"""
        cursor.execute(sql, (c_ids,))
        q = cursor.fetchall()
        for i in q:
            credit_detail.append({
                'bank': i[0],
                'currency': i[1],
                'amount': i[2],
                'used': i[3],
                'unused': i[4],
                'name': i[5],
                'limit_date': i[6]
            })
        
        result['credit_detail'] = credit_detail

        # 债券
        # 债券统计信息
        bond_total = {
            'rest': '',
            'total': '',
            'avg_deadline': '',
            'avg_inter_rate': '',
            'classify': [],
            'subject': []
        }
        sql = """select sum(rest), sum(total), avg(deadline), avg(inter_rate) 
        from b_bond where date(honour_date) > curdate() and
        debt_subject in %s"""
        cursor.execute(sql, (names,))
        q = cursor.fetchone()
        if q:
            bond_total['rest'] = q[0]
            bond_total['total'] = q[1]
            bond_total['avg_deadline'] = q[2]
            bond_total['avg_inter_rate'] = q[3]
        sql = """select classify, count(*), sum(rest), sum(total)
        from b_bond where date(honour_date) > curdate() and
        debt_subject in %s
        group by classify"""
        cursor.execute(sql, (names,))
        q = cursor.fetchall()
        for i in q:
            bond_total['classify'].append({
                'classify': i[0],
                'count': i[1],
                'rest': i[2],
                'total': i[3]
            })
        sql = """select debt_subject, sum(rest), sum(total)
        from b_bond where date(honour_date) > curdate() and
        debt_subject in %s
        group by debt_subject"""
        cursor.execute(sql, (names,))
        q = cursor.fetchall()
        for i in q:
            bond_total['subject'].append({
                'subject': i[0],
                'rest': i[1],
                'total': i[2]
            })

        result['bond_total'] = bond_total

        # 债券详细信息
        bond_detail = []
        rating = {
            'organ': '',
            'date': '',
            'rating': '',
            'move': ''
        }

        sql = """
        select debt_subject, s_id, s_name, s_short_name, init_face_value, total, rest, inter_type, 
            inter_rate, deadline, list_date, list_address, classify, lead_underwriter
        from b_bond where date(honour_date) > curdate() and
        debt_subject in %s
        order by list_date desc"""
        cursor.execute(sql, (names, ))
        q = cursor.fetchall()
        for i in q:
            item = {
                'debt_subject': i[0],
                's_id': i[1],
                's_name': i[2],
                's_short_name': i[3],
                'init_face_value': i[4],
                'total': i[5],
                'rest': i[6],
                'inter_type': i[7],
                'inter_rate': i[8],
                'deadline': i[9],
                'list_date': str(i[10]),
                'list_address': i[11],
                'classify': i[12],
                'lead_underwriter': i[13]
            }
            bond_detail.append(item)
            
        # 主体评级
        sql = """select s_id from b_bond where debt_subject = 
        (select name from c_client where c_id = %s)"""
        cursor.execute(sql, (g_id, ))
        q = cursor.fetchall()
        s_ids = []
        for i in q:
            s_ids.append(i[0])
        if s_ids:
            sql = """
            select organ, date(issuer_time), issuer_rating, issuer_move 
            from b_bond_rating where s_id in %s 
            order by date(issuer_time) desc
            limit 1"""
            cursor.execute(sql, (s_ids, ))
            q = cursor.fetchone()
            if q:
                rating['organ'] = q[0]
                rating['date'] = str(q[1])
                rating['rating'] = q[2]
                rating['move'] = q[3]

        result['bond_detail'] = bond_detail
        result['rating'] = rating

        # 有息负债
        debt = {
            'total': '',
            '3_years': [],
            'last': []
        }
        # 最近一期
        sql = """select sum(sum_liab), report_date, sum(st_borrow), sum(bill_pay), sum(non_curr_liab_one_year), 
        sum(lt_borrow), sum(bond_pay), sum(leasehold), sum(debt_total), sum(loan_rec), sum(issue_bond_rec), 
        sum(repay_debt_pay), sum(dividend_profit_or_interest_pay), sum(flow_total)
        from debt_info where report_type = 0 and c_id in %s
        group by report_date
        order by report_date desc
        limit 1"""
        cursor.execute(sql, (c_ids,))
        q = list(cursor.fetchone())
        if q:
            debt['total'] = q[0]
            debt['last'] = q[1:]
            debt['last'][0] = str(debt['last'][0])
        # 近三年
        sql = """select report_date, sum(st_borrow), sum(bill_pay), sum(non_curr_liab_one_year), 
        sum(lt_borrow), sum(bond_pay), sum(leasehold), sum(debt_total), sum(loan_rec), sum(issue_bond_rec), 
        sum(repay_debt_pay), sum(dividend_profit_or_interest_pay), sum(flow_total)
        from debt_info where report_type = 1 and c_id in %s
        group by report_date
        order by report_date desc
        limit 3"""
        cursor.execute(sql, (c_ids,))
        q = cursor.fetchall()
        for i in q:
            i = list(i)
            i[0] = str(i[0])
            debt['3_years'].append(i)

        result['debt'] = debt

        # 股权融资
        share_financing_total = {
            'listed_num': 0,
            'listed': [],
            'total': 0,
            'detail': ''
        }
        sql = """select name from c_client
        where is_listed = 1
        and group_id = %s"""
        cursor.execute(sql, g_id)
        q = cursor.fetchall()
        for i in q:
            share_financing_total['listed'].append(i[0])
        share_financing_total['listed_num'] = len(share_financing_total['listed'])

        sql = """select a_code, b_code, h_code, x_code, n_code, nas_code 
        from c_client where c_id in %s"""
        cursor.execute(sql, (c_ids,))
        q = cursor.fetchall()
        code_list = []
        for i in q:
            code_list.extend(i)
        code_list = set(code_list)
        codes = []
        for i in code_list:
            if i and re.match(r'\d{6}', i):
                codes.append(i)
        if codes:
            sql = """select issue_type, sum(issue_count*issue_price) from shares_financing
            where s_id in %s
            group by issue_type"""
            cursor.execute(sql, (codes,))
            q = cursor.fetchall()
            share_financing_total['detail'] = q
            for i in q:
                share_financing_total['total'] += i[1]
        
        result['share_financing_total'] = share_financing_total

        # 股权融资详细
        share_financing_detail = []
        if codes:
            sql = """select s_id, s_name, list_date, address, lead_underwriter
            from shares where s_id in %s"""
            cursor.execute(sql, (codes,))
            q = cursor.fetchall()
            for i in q:
                item = {
                    's_id': i[0],
                    's_name': i[1],
                    'list_date': i[2],
                    'address': i[3],
                    'lead_underwriter': i[4],
                    'detail': []
                }
                sql = """select issue_type, issue_date, issue_count, issue_price, raise_funds
                from shares_financing where s_id = %s"""
                cursor.execute(sql, i[0])
                tmp = cursor.fetchall()
                for tmp_i in tmp:
                    tmp_i = list(tmp_i)
                    tmp_i[1] = str(tmp_i[1])
                    item['detail'].append(tmp_i)

                share_financing_detail.append(item)

        result['share_financing_detail'] = share_financing_detail

        cursor.close()
    else:
        result['error'] = 'Need g_id'

    return jsonify(result)
