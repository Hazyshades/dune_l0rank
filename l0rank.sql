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
0x2a766f08754bf29bab10a6502ad5e3ef509bee58	,
0x0409edc34a704ecd9be0d5c1b2b3c39784fb5099	,
0x1e785a21f369b40ee8db7f409d3ffe1a3794b7c9	,
0x3dfa31b4fd4283b742526ff1ea6d9436e85f700f	,
0x14febd5fc3a92657a8d2cf3029a0746216accdd4	,
0x61ae575788822f6f51f98f01f2bf984e9b19a913	,
0x96d41bc0248f46ab1a4eeb2c5a9e7bb8a7b7a691	,
0xcaa3692f0c38f55d0c7414c7cba283484bd6a48f	,
0x9b3fd4cddf505b64530d8329e4c0f153745c6d21	,
0xe30560f03429965bac77b39c6200ddc511b28ed8	,
0x046756f49078b405f6c5bfe4be78999254aabbae	,
0x45ebc4cc551297eacb59a4db00dfce2720c43250	,
0x471da1d84175fd16b30d39dccc91fe214690a143	,
0x3584ccfb868d7f2b083870db865bd6b447741fef	,
0x38e7217da3bed3f032e1e9361711e21ee9196d93	,
0xb6c4369cbb960be1870c7e7780c2e53300e464b7	,
0x6edc080130b63d7b2793dfb3cd8c8f33532f636c	,
0xba68186d3b4f7fb552ef2af6134090777d008de3	,
0xfb2099c9ec1f2c45b8945a4d79e96beccbe28b13	,
0x449e5072ca5f9a47c0fe6339e34d280c634d789b	
)
order by rank_score desc, amount_usd desc, transaction_count desc
