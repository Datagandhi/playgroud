with

    salesdata as (
        select
            opportunitynumber as opportunity_number,
            usi,
            ctn,
            activated_date as connection_date,
            round(monthly_recurring_charge, 2) as line_rental,
            network,
            currentorderstatus as current_order_status,
            product_segment
        from {{ source("peakai", "ao_internal") }}
        where
            currentorderstatus in ('Activated', 'Exchange', 'Exchange Received')
            and activated_date is not null
            and network = 'O2'
    ),
    paymentdata as (
        select
            usi,
            period_date,
            period_date_converted,
            mpn,
            lvl5,
            sos_code,
            sum(commission_value) as commission_value
        from
            (
                select
                    usi,
                    period_date,
                    date_trunc(
                        'MONTH', try_to_date(period_date, 'MON-YY')
                    ) as period_date_converted,
                    commission_value,
                    mpn,
                    lvl5,
                    sos_code
                from {{ source("peakai", "postpay_revshare") }}
                where
                    revenue_feature_group_name = 'Line Rental'
                    and description = 'In Contract-71%'

                union all
                select
                    usi,
                    periodname as period_date,
                    date_trunc(
                        'MONTH', try_to_date(periodname, 'MON-YY')
                    ) as period_date_converted,
                    commissionvalue as commission_value,
                    mpn,
                    level5 as lvl5,
                    soscode as sos_code
                from {{ source("peakai", "mpd_ftu_compensation") }}
            ) t
        group by usi, period_date, period_date_converted, mpn, lvl5, sos_code
        order by period_date_converted

    ),
    commissions as (
        select vat, commission_rate_1, commission_rate_2,active_date,end_date
        from {{ source("peakai", "commission_reconciliation_rate") }}
        where operator = 'O2'
    ),
    disconnection as (
        select usi, disconnection_date, disconnection_reason
        from {{ source("peakai", "mpd_disconnection_report") }}
    ),
    basemovementupgrade as (
        select
            usi,
            to_date(last_upgrade_date) as last_upgrade_date,
            upgrade_channel,
            base_indicator
        from {{ source("peakai", "mpd_base_movement_report") }}
        where base_indicator = 'BASE OFF'
    ),
    basemovementnonbilling as (
        select
            usi,
            period,
            billed_status,
            date_trunc(
                'MONTH', to_date(to_char(period), 'yyyymm')
            ) as periodstartofmonth
        from {{ source("peakai", "mpd_base_movement_report") }}
    ),
    monthseries as (
        select
            fs.opportunitynumber,
            fs.usi,
            fs.ctn,
            fs.connectiondate,
            fs.linerental,
            fs.network,
            fs.product_segment,
            dateadd(
                month, n.number, to_date(to_char(connectiondate, 'YYYY-MM') || '-01')
            ) as claimmonth,
            {{ get_start_of_fy('CLAIMMONTH') }} as claim_start_of_fy,
            n.number + 1 as monthnumber
        from salesdata fs
        join
            (
                select row_number() over (order by seq4()) - 1 as number
                from table(generator(rowcount => 36))
            ) n
            on true
        -- where
        --     dateadd(month, n.number, connectiondate) <= (
        --         select
        --             last_day(
        --                 max(perioddateconverted)
        --             ) as max_month_eom
        --         from paymentdata
        --     )
    ),
    commissionwithrpi as (
        select {{ calculate_rpi_increase(
    network="'O2'",
    connection_date="CONNECTIONDATE",
    claim_month="CLAIMMONTH",
    base_mrc="LINERENTAL",
    claim_start_of_fy = "CLAIM_START_OF_FY"
) }} AS uplifted_mrc,ms.*, ms.linerental as linerentaladjusted from monthseries ms
    ),
    commissioncalc as (
        select
            ms.*,
            c.vat,
            c.commission_rate_1,
            c.commission_rate_2,
            round(ms.linerentaladjusted / 1+c.vat, 2) as netmrc,
            case
                when monthnumber between 1 and 24
                then (ms.linerentaladjusted * c.commission_rate_1) / c.vat
                when monthnumber between 25 and 36
                then (ms.linerentaladjusted * c.commission_rate_2) / c.vat
            end as expectedcommission_base
        from commissionwithrpi ms
        left join commissions c on c.active_date > ms.claimmonth and c.end_date >= ms.claimmonth
    ),
    prorated as (
        select
            *,
            round(
                case
                    when monthnumber = 1 and day(connectiondate) > 1
                    then
                        round(
                            (
                                datediff(day, connectiondate, last_day(connectiondate))
                                + 1
                            )
                            * expectedcommission_base
                            / day(last_day(connectiondate)),
                            4
                        )
                    else expectedcommission_base
                end,
                2
            ) as expectedcommission
        from commissioncalc
    ),
    withdisconnection as (
        select
            pr.*,
            dis.disconnection_date,
            dis.disconnection_reason,
            round(
                case
                    when dis.disconnection_date < claimmonth
                    then 0
                    when
                        dis.disconnection_date >= claimmonth
                        and month(dis.disconnection_date) = month(claimmonth)
                        and year(dis.disconnection_date) = year(claimmonth)
                    then
                        expectedcommission * (
                            day(dis.disconnection_date)
                            / cast(day(last_day(claimmonth)) as float)
                        )
                    else expectedcommission
                end,
                2
            ) as expectedcommission_afterdisconnection
        from prorated pr
        left join disconnection dis on pr.usi = dis.usi
    ),
    withupgrades as (
        select
            wd.*,
            wu.last_upgrade_date,
            wu.upgrade_channel,
            wu.base_indicator,
            round(
                case
                    when
                        wu.last_upgrade_date is not null
                        and datediff(month, connectiondate, wu.last_upgrade_date) >= 21
                    then 0
                    else expectedcommission_afterdisconnection
                end,
                2
            ) as expectedcommission_afterupgrade,
            case
                when
                    wu.last_upgrade_date is not null
                    and datediff(month, connectiondate, wu.last_upgrade_date) < 21
                then 'FTU'
                else null
            end as ftu_flag
        from withdisconnection wd
        left join basemovementupgrade wu on wd.usi = wu.usi
    ),
    withnonbilling as (
        select
            wu.*,
            bmb.billed_status,

            round(
                case
                    when
                        bmb.billed_status = 'NON BILLED'
                        and claimmonth
                        = bmb.periodstartofmonth
                    then 0
                    else expectedcommission_afterupgrade
                end,
                2
            ) as finalexpectedcommission

        from withupgrades wu
        left join
            basemovementnonbilling bmb
            on wu.usi = bmb.usi
            and wu.claimmonth = bmb.periodstartofmonth
    ),
    withpayments as (
        select
            wn.opportunitynumber,
            wn.usi,
            wn.ctn,
            wn.connectiondate,
            wn.linerentaladjusted,
            wn.network,
            wn.claimmonth,
            wn.monthnumber,
            wn.product_segment,
            wn.netmrc,
            wn.expectedcommission_base,
            wn.expectedcommission,
            wn.disconnection_date,
            wn.disconnection_reason,
            wn.expectedcommission_afterdisconnection,
            wn.last_upgrade_date,
            wn.upgrade_channel,
            wn.base_indicator,
            wn.expectedcommission_afterupgrade,
            wn.ftu_flag,
            wn.billed_status,
            wn.finalexpectedcommission,
            concat(cast(wn.usi as string), cast(pay.period_date as string)) as usidate,
            count(pay.commission_value) as payment_count,
            sum(pay.commission_value) as commissionpaid,
            round(
                coalesce(sum(abs(pay.commission_value)), 0)
                - coalesce(wn.finalexpectedcommission, 0),
                2
            ) as variance,
            pay.mpn,
            pay.lvl5,
            pay.sos_code,
            coalesce(pay.perioddateconverted, wn.claimmonth) as inputfilter
        from withnonbilling wn
        left join
            paymentdata pay
            on wn.usi = pay.usi
            and wn.claimmonth = pay.perioddateconverted
        group by
            wn.opportunitynumber,
            wn.usi,
            wn.ctn,
            wn.connectiondate,
            wn.linerentaladjusted,
            wn.network,
            wn.claimmonth,
            wn.monthnumber,
            wn.product_segment,
            wn.netmrc,
            wn.expectedcommission_base,
            wn.expectedcommission,
            wn.disconnection_date,
            wn.disconnection_reason,
            wn.expectedcommission_afterdisconnection,
            wn.last_upgrade_date,
            wn.upgrade_channel,
            wn.base_indicator,
            wn.expectedcommission_afterupgrade,
            wn.ftu_flag,
            wn.billed_status,
            wn.finalexpectedcommission,
            pay.period_date,
            pay.mpn,
            pay.lvl5,
            pay.sos_code,
            pay.perioddateconverted
    ),
    withdisconnectionadjustments as (
        select
            wp.*,
            case
                when disconnection_date is null
                then finalexpectedcommission
                when disconnection_date > last_day(claimmonth)
                then finalexpectedcommission
                else 0
            end as adjustedcommission
        from withpayments wp
    ),

    withclaims as (
        select
            wda.*,
            case
                when adjustedcommission = 0
                then 'Customer Disconnected'
                when coalesce(commissionpaid, 0) = 0 and adjustedcommission > 0
                then 'Missing Payment'
                when variance > -.10 and variance < .10
                then 'Paid'
                when variance >= 0.05
                then 'Overpaid'
                when commissionpaid < adjustedcommission
                then 'Underpaid'
                else 'No Claim'
            end as claimtype,

            case
                when coalesce(commissionpaid, 0) = 0 and adjustedcommission > 0
                then
                    case
                        when
                            date_trunc('MONTH', last_upgrade_date) >= connectiondate
                            and date_trunc('MONTH', last_upgrade_date)
                            <= date_trunc('MONTH', claimmonth)
                        then 'Customer Upgraded'

                        when billed_status = 'NON BILLED'
                        then 'Customer not billed'

                        else 'Missing Payment'
                    end
                else null  -- or use ClaimType, or another fallback
            end as paymentstatus,
        from withdisconnectionadjustments wda
    ),

    tmpval as (
        select distinct
            usi,
            min(claimmonth) as firstmissingrevenuesharemonth,
            max(claimmonth) as lastmissingrevsharemonth
        from withclaims
        where claimtype in ('Missing Payment', 'Underpaid')
        group by usi
    ),
    tmpval1 as (
        select distinct
            usi,
            min(claimmonth) as firstmissingrevenuesharemonth,
            max(claimmonth) as lastmissingrevsharemonth
        from withclaims
        where claimtype in ('Missing Payment', 'Underpaid') and ftu_flag = 'FTU'
        group by usi
    )

-- select
--     paymentstatus,
--     wc.usi,
--     ctn as mpn,
--     opportunitynumber as ordernumber,
--     connectiondate,
--     claimtype,
--     claimmonth,
--     case
--         when ftu_flag = 'FTU' then 'FTU Compensation' else 'Postpay Revenue Share'
--     end as paymenttype,
--     case
--         when product_segment = 'New Contract'
--         then 'Connection'
--         when product_segment = 'Upgrade'
--         then 'Resign'
--         else product_segment
--     end as eventtype,
--     adjustedcommission as expectedtotalvalue,
--     commissionpaid as valuepaid,
--     tmpval.firstmissingrevenuesharemonth,
--     tmpval.lastmissingrevsharemonth as revenuesharemonthmissingto,
--     tmpval1.firstmissingrevenuesharemonth as firstmissingftucompensationmonth,
--     tmpval1.lastmissingrevsharemonth as ftucompensationmonthmissingto,
--     case
--         when claimtype = 'Missing Payment'
--         then 'No payment found'
--         when claimtype = 'Underpaid'
--         then
--             'Underpaid by '
--             || cast(round(expectedcommission, 2) - round(commissionpaid, 2) as string)
--         else 'Paid in full'
--     end as comment,
--     coalesce(lvl5, '5L6890') as level5code,
--     coalesce(sos_code, 'R23') as soscode,
--     inputfilter
-- from withclaims wc
-- left join tmpval on tmpval.usi = wc.usi
-- left join tmpval1 on tmpval1.usi = wc.usi
-- where
--     (
--         tmpval.firstmissingrevenuesharemonth is not null
--         or tmpval1.firstmissingrevenuesharemonth is not null
--     )
--     and claimtype in ('Missing Payment', 'Underpaid')  -- AND inputfilter BETWEEN :P_STARTDATE AND :P_ENDDATE;

select *
from commissionwithrpi where opportunitynumber = '50106509.00' order by claimmonth asc