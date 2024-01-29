-- select *
-- from query_2465489
-- order by rank_id
-- limit 1000000

with user_summary as (
    select user_address, 
        count(*) as transaction_count,
        min(block_time) as initial_block_time,
        max(block_time) as last_block_time,
        date_diff('day', min(block_time), now()) as lz_age_days,
        count(distinct source_chain_id) as active_source_chain_count,
        count(distinct destination_chain_id) as active_destination_chain_count,
        count(distinct transaction_contract) as active_transaction_contract_count,
        count(distinct date_trunc('day', block_time)) as active_days_count,
        count(distinct date_trunc('week', block_time)) as active_weeks_count,
        count(distinct date_trunc('month', block_time)) as active_months_count,
        -- coalesce(sum(amount_usd / power(10, p.decimals) * p.price), 0) as amount_usd
        coalesce(sum(amount_usd), 0) as amount_usd
    from layerzero.send
    group by 1
),

user_summary_with_rank as (
    select *,
        (
        active_source_chain_count -- Count of used source chains
        
        + if(active_destination_chain_count >= 2, 1, 0)  -- Conducted transactions to 2 destination chains
        + if(active_destination_chain_count >= 5, 1, 0)  -- Conducted transactions to 5 destination chains
        + if(active_destination_chain_count >= 10, 1, 0)  -- Conducted transactions to 10 destination chains
        
        + if(active_months_count >= 2, 1, 0)  -- Conducted transactions during 2 distinct months
        + if(active_months_count >= 6 , 1, 0) -- Conducted transactions during 6 distinct months
        + if(active_months_count >= 9, 1, 0)  -- Conducted transactions during 9 distinct months
        + if(active_months_count >= 12, 1, 0)  -- Conducted transactions during 12 distinct months
        
        + if(active_weeks_count >= 10, 1, 0)  -- Conducted transactions during 10 distinct weeks
        + if(active_weeks_count >= 20, 1, 0)  -- Conducted transactions during 20 distinct weeks
        + if(active_weeks_count >= 50, 1, 0)  -- Conducted transactions during 50 distinct weeks
        + if(active_weeks_count >= 100, 1, 0)  -- Conducted transactions during 100 distinct weeks
        
        + if(active_days_count >= 50, 1, 0)  -- Conducted transactions during 50 distinct days
        + if(active_days_count >= 100, 1, 0)  -- Conducted transactions during 100 distinct days
        + if(active_days_count >= 200, 1, 0)  -- Conducted transactions during 200 distinct days
        + if(active_days_count >= 500, 1, 0)  -- Conducted transactions during 500 distinct days
        
        
        + if(lz_age_days >= 100, 1, 0)  -- Started using Layer Zero before 100 days
        + if(lz_age_days >= 200, 1, 0)  -- Started using Layer Zero before 200 days
        + if(lz_age_days >= 500, 1, 0)  -- Started using Layer Zero before 500 days
        
        + if(transaction_count >= 5, 1, 0)  -- Conducted more than 5 transactions
        + if(transaction_count >= 10, 1, 0)  -- Conducted more than 10 transactions
        + if(transaction_count >= 25, 1, 0)  -- Conducted more than 25 transactions
        + if(transaction_count >= 50, 1, 0)  -- Conducted more than 50 transactions
        + if(transaction_count >= 100, 1, 0)  -- Conducted more than 100 transactions
        
        + if(active_transaction_contract_count >= 5, 1, 0)  -- Interacted more than 5 contracts on source chain
        + if(active_transaction_contract_count >= 10, 1, 0)  -- Interacted more than 10 contracts on source chain
        + if(active_transaction_contract_count >= 25, 1, 0)  -- Interacted more than 25 contracts on source chain
        + if(active_transaction_contract_count >= 50, 1, 0)  -- Interacted more than 100 contracts on source chain
        + if(active_transaction_contract_count >= 100, 1, 0)  -- Interacted more than 100 contracts on source chain
        
        + if(amount_usd > 0, 1, 0) -- Bridged funds through Layer Zero
        + if(amount_usd > 1000, 1, 0) -- Bridged more than $1,000 of assets through Layer Zero
        + if(amount_usd > 10000, 1, 0) -- Bridged more than $10,000 of assets through Layer Zero
        + if(amount_usd > 50000, 1, 0) -- Bridged more than $50,000 of assets through Layer Zero
        + if(amount_usd > 250000, 1, 0) -- Bridged more than $250,000 of assets through Layer Zero
        + if(amount_usd > 500000, 1, 0) -- Bridged more than $500,000 of assets through Layer Zero
        + if(amount_usd > 1000000, 1, 0) -- Bridged more than $1,000,000 of assets through Layer Zero
        ) as rank_score
    from user_summary
)

select row_number() over (order by rank_score desc, amount_usd desc, transaction_count desc) as Rank,
    user_address as Address,
    transaction_count as TXs,
    round(amount_usd, 2) as Amount,
    cast(active_source_chain_count as varchar) || ' / ' || cast(active_destination_chain_count as varchar) || ' / ' || cast(active_transaction_contract_count as varchar) as Active_source_chain_count,
    cast(active_days_count as varchar) || ' / ' || cast(active_weeks_count as varchar) || ' / ' || cast(active_months_count as varchar) as Active_days_count,
    lz_age_days as Wallet_age,
    initial_block_time as InitialTx,
    last_block_time  as LastTx
from user_summary_with_rank
where user_address in (
0x57...,
)
order by rank_score desc, amount_usd desc, transaction_count desc
