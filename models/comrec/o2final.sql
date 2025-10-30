with

    salesdata as (
        select
            opportunitynumber as opportunity_number,
            usi,
            ctn,
            activated_date as connection_date,
            round(monthly_recurring_charge, 2) as monthly_recurring_charge,
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
        select vat, commission_rate_1, commission_rate_2, active_date, end_date
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
            to_date(last_upgrade_date, 'DD/MM/YYYY HH24:MI') as last_upgrade_date,
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
            ) as period_start_of_month
        from {{ source("peakai", "mpd_base_movement_report") }}
    ),
    monthseries as (
        select
            fs.opportunity_number,
            fs.usi,
            fs.ctn,
            fs.connection_date,
            fs.monthly_recurring_charge as gross_mrc,
            fs.network,
            fs.product_segment,
            dateadd(
                month, n.number, to_date(to_char(connection_date, 'YYYY-MM') || '-01')
            ) as claim_month,
            n.number + 1 as month_number,
            {{ get_start_of_fy("claim_month") }} as claim_start_of_fy,
            date_trunc(month, claim_month) as start_of_claim_month,
            dateadd(month, 1, start_of_claim_month) as start_of_next_claim_month,
            datediff(
                day, start_of_claim_month, start_of_next_claim_month
            ) as days_in_month,

            case
                when month_number in (1, 25)
                then
                    days_in_month - datediff(
                        day,
                        start_of_claim_month,
                        dateadd(month, (month_number - 1), connection_date)
                    )
                else null
            end as days_pro_rata,
            {{
                calculate_rpi_increase(
                    network="'O2'",
                    connection_date="connection_date",
                    claim_month="claim_month",
                    base_mrc="gross_mrc",
                    claim_start_of_fy="claim_start_of_fy",
                )
            }} as uplifted_mrc,
            round(
                case
                    when month_number = 1 and day(connection_date) > 1
                    then round(uplifted_mrc * (days_pro_rata / days_in_month), 4)
                    else uplifted_mrc
                end,
                2
            ) as uplifted_mrc_pro_rata,
            round(uplifted_mrc_pro_rata / (1 + c.vat), 2) as net_mrc,
            round(case
                when month_number between 1 and 24
                then (net_mrc * c.commission_rate_1)
                when month_number between 25 and 36
                then (net_mrc * c.commission_rate_2)
            end,2) as expected_commission
        from salesdata fs
        join
            (
                select row_number() over (order by seq4()) - 1 as number
                from table(generator(rowcount => 36))
            ) n
            on true
        left join
            commissions c
            on c.active_date <= claim_month
            and c.end_date >= claim_month
        where
            dateadd(month, n.number, connection_date) <= (
                select last_day(max(period_date_converted)) as max_month_eom
                from paymentdata
            )
    ),
    withdisconnection as (
        select
            pr.*,
            dis.disconnection_date,
            dis.disconnection_reason,
            round(
                case
                    when dis.disconnection_date < claim_month
                    then 0
                    when
                        dis.disconnection_date >= claim_month
                        and month(dis.disconnection_date) = month(claim_month)
                        and year(dis.disconnection_date) = year(claim_month)
                    then
                        expected_commission * (
                            day(dis.disconnection_date)
                            / cast(day(last_day(claim_month)) as float)
                        )
                    else expected_commission
                end,
                2
            ) as expected_commission_after_disconnection
        from monthseries pr
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
                        and datediff(month, connection_date, wu.last_upgrade_date) >= 21
                    then 0
                    else expected_commission_after_disconnection
                end,
                2
            ) as expected_commission_after_upgrade,
            case
                when
                    wu.last_upgrade_date is not null
                    and datediff(month, connection_date, wu.last_upgrade_date) < 21
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
                        and claim_month = bmb.period_start_of_month
                    then 0
                    else expected_commission_after_upgrade
                end,
                2
            ) as final_expected_commission

        from withupgrades wu
        left join
            basemovementnonbilling bmb
            on wu.usi = bmb.usi
            and wu.claim_month = bmb.period_start_of_month
    ),
    withpayments as (
        select
            wn.opportunity_number,
            wn.usi,
            wn.ctn,
            wn.connection_date,
            wn.gross_mrc,
            wn.network,
            wn.claim_month,
            wn.month_number,
            wn.product_segment,
            wn.net_mrc,
            wn.expected_commission,
            wn.disconnection_date,
            wn.disconnection_reason,
            wn.expected_commission_after_disconnection,
            wn.last_upgrade_date,
            wn.upgrade_channel,
            wn.base_indicator,
            wn.expected_commission_after_upgrade,
            wn.ftu_flag,
            wn.billed_status,
            wn.final_expected_commission,
            concat(cast(wn.usi as string), cast(pay.period_date as string)) as usi_date,
            count(pay.commission_value) as payment_count,
            sum(pay.commission_value) as commission_paid,
            round(
                coalesce(sum((pay.commission_value)), 0)
                - coalesce(wn.final_expected_commission, 0),
                2
            ) as variance,
            pay.lvl5,
            pay.sos_code,
            coalesce(pay.period_date_converted, wn.claim_month) as input_filter
        from withnonbilling wn
        left join
            paymentdata pay
            on wn.usi = pay.usi
            and wn.claim_month = pay.period_date_converted
        group by
            wn.opportunity_number,
            wn.usi,
            wn.ctn,
            wn.connection_date,
            wn.gross_mrc,
            wn.network,
            wn.claim_month,
            wn.month_number,
            wn.product_segment,
            wn.net_mrc,
            wn.expected_commission,
            wn.disconnection_date,
            wn.disconnection_reason,
            wn.expected_commission_after_disconnection,
            wn.last_upgrade_date,
            wn.upgrade_channel,
            wn.base_indicator,
            wn.expected_commission_after_upgrade,
            wn.ftu_flag,
            wn.billed_status,
            wn.final_expected_commission,
            pay.period_date,
            pay.lvl5,
            pay.sos_code,
            pay.period_date_converted
    ),
    withdisconnectionadjustments as (
        select
            wp.*,
            case
                when disconnection_date is null
                then final_expected_commission
                when disconnection_date > last_day(claim_month)
                then final_expected_commission
                else 0
            end as adjusted_commission
        from withpayments wp
    ),

    withclaims as (
        select
            wda.*,
            case
                when adjusted_commission = 0
                then 'Customer Disconnected'
                when coalesce(commission_paid, 0) = 0 and adjusted_commission > 0
                then 'Missing Payment'
                when variance > -.10 and variance < .10
                then 'Paid'
                when variance >= 0.05
                then 'Overpaid'
                when commission_paid < adjusted_commission
                then 'Underpaid'
                else 'No Claim'
            end as claim_type,

            case
                when coalesce(commission_paid, 0) = 0 and adjusted_commission > 0
                then
                    case
                        when
                            date_trunc('MONTH', last_upgrade_date) >= connection_date
                            and date_trunc('MONTH', last_upgrade_date)
                            <= date_trunc('MONTH', claim_month)
                        then 'Customer Upgraded'

                        when billed_status = 'NON BILLED'
                        then 'Customer not billed'

                        else 'Missing Payment'
                    end
                else null  -- or use claim_type, or another fallback
            end as payment_status,case
                when coalesce(commission_paid, 0) = 0 and adjusted_commission > 0 then 1 else 0 end as test
        from withdisconnectionadjustments wda
    ),

    tmpval as (
        select distinct
            usi,
            min(claim_month) as first_missing_revenue_share_month,
            max(claim_month) as last_missing_revshare_month
        from withclaims
        where claim_type in ('Missing Payment', 'Underpaid')
        group by usi
    ),
    tmpval1 as (
        select distinct
            usi,
            min(claim_month) as first_missing_revenue_share_month,
            max(claim_month) as last_missing_revshare_month
        from withclaims
        where claim_type in ('Missing Payment', 'Underpaid') and ftu_flag = 'FTU'
        group by usi
    )

select
payment_status,
wc.usi,
ctn as mpn,
opportunity_number as ordernumber,
connection_date,
claim_type,
claim_month,
case
when ftu_flag = 'FTU' then 'FTU Compensation' else 'Postpay Revenue Share'
end as paymenttype,
case
when product_segment = 'New Contract'
then 'Connection'
when product_segment = 'Upgrade'
then 'Resign'
else product_segment
end as eventtype,
adjusted_commission as expectedtotalvalue,
commission_paid as valuepaid,
tmpval.first_missing_revenue_share_month,
tmpval.last_missing_revshare_month as revenuesharemonthmissingto,
tmpval1.first_missing_revenue_share_month as firstmissingftucompensationmonth,
tmpval1.last_missing_revshare_month as ftucompensationmonthmissingto,
case
when claim_type = 'Missing Payment'
then 'No payment found'
when claim_type = 'Underpaid'
then
'Underpaid by '
|| cast(round(expected_commission, 2) - round(commission_paid, 2) as string)
else 'Paid in full'
end as comment,
coalesce(lvl5, '5L6890') as level5code,
coalesce(sos_code, 'R23') as soscode,
input_filter
from withclaims wc
left join tmpval on tmpval.usi = wc.usi
left join tmpval1 on tmpval1.usi = wc.usi
where
(
tmpval.first_missing_revenue_share_month is not null
or tmpval1.first_missing_revenue_share_month is not null
)
and claim_type in ('Missing Payment', 'Underpaid')  -- AND input_filter BETWEEN
-- -- :P_STARTDATE AND :P_ENDDATE;


-- select *
-- from withclaims
-- where usi = '430016521'
-- order by claim_month asc
