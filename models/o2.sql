with

    salesdata as (
        select
            opportunitynumber,
            usi,
            ctn,
            connectiondate,
            round(linerental, 2) as linerental,
            network,
            currentorderstatus,
            product_segment
        from ao_tech_silver.peakai.ao_internal
        where
            currentorderstatus in ('Activated', 'Exchange', 'Exchange Received')
            and connectiondate is not null
            and network = 'O2'
    ),
    monthseries as (
        select
            fs.opportunitynumber,
            dateadd(
                month, n.number, to_date(to_char(connectiondate, 'YYYY-MM') || '-01')
            ) as claim_month,
            fs.usi,
            fs.ctn,
            fs.connectiondate,
            fs.linerental,
            fs.network,
            fs.product_segment,
            dateadd(
                month, n.number, to_date(to_char(connectiondate, 'YYYY-MM') || '-01')
            ) as claimmonth,
            {{ get_fy_month('CLAIMMONTH') }} as claim_fy_month,
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
    claim_fy_month = "CLAIM_FY_MONTH"
) }} AS adjusted_mrc,ms.*, ms.linerental as linerentaladjusted from monthseries ms
    )

select * from commissionwithrpi where opportunitynumber = '50106509.00' order by claimmonth