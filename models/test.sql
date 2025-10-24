round(

    linerental * case
        when
            connectiondate < to_date('2022-04-01')
            and claim_fy_month >= to_date('2022-04-01')
        then 1.117
        else 1.0
    end
    * case
        when
            connectiondate < to_date('2023-04-01')
            and claim_fy_month >= to_date('2023-04-01')
        then 1.173
        else 1.0
    end
    * case
        when
            connectiondate < to_date('2024-04-01')
            and claim_fy_month >= to_date('2024-04-01')
        then 1.088
        else 1.0
    end
    + case
        when claim_fy_month >= to_date('2025-04-01')
        then
            floor(datediff(month, to_date('2025-04-01'), claim_fy_month) / 12) * 1.8
            + 1.8
        else 0
    end,
    2
)
