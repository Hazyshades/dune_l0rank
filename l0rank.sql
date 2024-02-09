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
0x5ca77043904A071B385e01C9a3eAED4CD5C8C126	,
0x397AcB950864ec7C11297873d8a519EbB72BF1C5	,
0x3cB915BfC4Fa5aC2C42B4263a27D66a7f416E8E3	,
0x94dd3d4572f5eb6b646a8e70dd88842176aa3dd5	,
0xa98129c44b29652bd45d6fa8cc8f7abd15d81de0	,
0x21052ab071b9b758973469289736c111b8848e98	,
0x85c66ec46b21502df1b9dc5bc9998a5ed8d206d8	,
0x51425087ff2f9377a92d2ae0346873b861d71450	,
0x3ac86ffb0c4185babe5314929fecc4472acf9f66	,
0x64abdaa4066a36b4bba01f634bfef9dfb45b569f	,
0x39308a29905083e2b7506b6477d1b3b52331b1b2	,
0xA4646a201334Ca0c2F24Aac7C8547Df3114f554B	,
0x49080BD11256d2C4d7220dAc5f4e985fc7d44E0C	,
0x46419aba5385845b612128542Ab70b643187f245	,
0x70a43FE6cF695A54cDa5e6b4A4235dCEcf11375B	,
0x60479D2fa555214884218F5F96faCe0B3E982B3f	,
0xFdaAec0487633C1e86a2769385454115d2bc40E9	,
0xC21Cdc50796Ec831ad6b0198819C1B287cC2caFD	,
0xeF417e4a8713E86E1204E8fa3230bB7e972acB64	,
0x30BCf72ce9374d90086c01F2fd0374aAE54e7A85	,
0x4B4c59b89dDc5F1b373b0be217765b2565Cd154A	,
0x5b73a2E86FAB6aEe651afe86804044Efa8D4c84b	,
0x45cc58974999af3105531366F441C5882B458f7f	,
0xcDD7Ee8A88A71c7eb6c870A1d8d1912e5C701242	,
0x63208A0F118a1EfBAF28bA3254f46aEBF6f24f59	,
0x8484242149B38c48Bfc502464e04f0d3Bdd640d1	,
0x96870A3B1Ca5e413e46aA723035Ae94C027cEf94	,
0x041A4aE3633E27f65b41B76570296Db1CA142E88	,
0x8B4F244C674911AAE96AbEf5B6eaC3D45E86E720	,
0x645746b48f5d05c7Ec920772cB0E0370764e5945	,
0x5aDd25abAc9F7512c58f8991e30406Ad2aBa7B0C	,
0xf2Aa15f5Edc98158Ed677C43d9a8e5dC8e17BC01	,
0xceB4aE2825B413aa512ed0C865D7Ff5094cF30A2	,
0x5f74D308b6D65100a74A9417c24B0AE703b287ee	,
0xb8b991AE0447bE83d51ca86dCaA3638Ccb19D42e	,
0xa080de1B9454e47085487962991B651a24042073	,
0x80B16Dd036c79821188Df1e5128932016BD71e77	,
0xC52C1281B5F3c979684957F3e302E589C3C1Eac1	,
0x6CC4cB5C9f69426bDdeDc302885956514a1135e3	,
0x99BFC6977b04f22EEF9f0DdB905AC29e2Cb24713	,
0xfc741EAdDa03CB60952FA0834A7035621D30b8cD	,
0x227445a4CEDdac179275F09B0f7c71e9fa23775f	,
0x228628bC50a52e2357A276a0A6d1486A4cDc7319	,
0x19F3eD3f57B0Fa97dDd4EC506a5188C595317C5E	,
0xFF7B2C1AE816E836114727d072E1C03d37a44433	,
0x2baF795e3faB519A2b1e5895c7275C3777346FEA	,
0xe8250A853B44A16D007337b91df1149ec3B86d16	,
0x69c9D03c1A77c6c4745Bd1CE6Eeb2FA6fE6a02A3	,
0xd07d6fdDfD581AA496fbCc49969AfaE7772437Bc	,
0x9B690c340718E3CcB1C0b3B9653886f0E4663a04	,
0x09Ef3D285696363AC6395101513FB49B4a6d38Fc	,
0x653245df2483E3B019d0d618b80105Ff79b66204	,
0x3Da32c6F0FcFb7A3078AD4FF1f238bE3E112676c	,
0x841Cb2B0A933b6cb86E0fD18f0e2763C6Ce552E7	,
0x557C45B7983aCF2481C74a83Fd52c05551506822	,
0x4212B3E786e1A65f646705f2614Ce47b664A19e0	,
0x6Ad21898324F7E05B31e42CFA932Ce2DFd98E316	,
0x1203FCF7dAD70E6B4E6505f76523dBd87D87c3F5	,
0xe4de0af9299167D4edA225D6B76D7E6635782E7F	,
0xf69d5Aae222406ee4Df7E0e4C31ea54638EBc901	,
0x250AF71b23a605f6d77DE7db18907b3A0c16391A	,
0x27e11587546c48e807d30175787e7eb4fd32028a	,
0x891888cFaBA25E44D4746850F1F2E6a261f28074	,
0x3f99A4DAc0C8A8542917C3250bE14446149DD5a2	,
0x273994219E6C0AD4B9dF71c67D062a0789FCFEAE	,
0xc7aFEA509e49654638Be2D1bfA2BFAF9253cEE10	,
0xe37725778eB03f9D3CE259aCb5Bf81e95Cd457e3	,
0xb1329bc38727b104ecbb26cb9be50371c4ed7a73	,
0x59b6dC61eaddcd5867F9702d1BE1e9faB983C136	,
0xcf3139dd48873b03bbc602eff90edd116782b734	,
0xbb6ddcbb214db09a76c7115beb1a207756ef9682	,
0x24ab5033587724b4b894cca35d6bc4ca1f408b3a	,
0xe5a83816f9ea8cc0134168ba642478579de67598	,
0x61eae1bb725e16529c16144db611226878aab81a	,
0xeb80bfa90eae2242e3cc99c1dcf81a1a39973152	,
0x98dbb179a5e9ae5227509a5cdb1201337b3eb7d6	,
0xfdd69ed53e55da83a76d56a3232f736551de1012	,
0xccbb7592ff6814d0eee4ca2aeb6ec9180349d0a5	,
0x8d09c739a650d8a60b8c2287c8e8cbc1d10a4108	,
0x757c0d2443f589ea6e42eccf5b7262f8e5a1f07d	,
0x74608b7c6cd422887d2b97970f30b3e958f9538b	,
0x1ed41e25be04f6b742aaf663131ac9c8a9b8d46c	,
0x81360d553ed8ad7331ba15b99adf0d151134264b	,
0xf4431db68940c4721087eff6ad013903ea4346ed	,
0x8472b6fc7b53faabaa3735d42f1a063d6a143c30	,
0x266415e6c8ce81961fbaf01fa4981e5fc25f23e2	,
0xc6de017515a9e918ecfe82f08dba984c84610f8c	,
0x81d163f77437d0825878caf06d17cd43211868b6	,
0x6e50aeee0db5522184d260df0003ff0bc0201bca	,
0x163bed609853568ff18e195693892d3adf904c86	,
0xbeec8427240dff5c570bee8ba1a7d2cff0608fbc	,
0x0e58f284fa230a019889e789d2fe2abe5e338cc7	,
0x6da861049597e7c584ed7e39b95cd544b9b90cd5	,
0x971f207a1e778c2e3fbe618e07c00deaf3c97262	,
0xb05b040edba5b21257ee3b5935701b62d1becfc8	,
0xcf5423966e2fac51d40c79d4be6d308bd89242db	,
0x6b7ea6ebbe403df5497efe1532a17877f93e4e46	,
0xdb02de9f4b97b56156103312abff02a90d885358	,
0xf9601897b35706a77dc18d340edc36923f51ba71	,
0xd90589e3d7ee8347422aec84d0e5568f9866402e	,
0x0fb06d2c2bc42e8a31c69acb96e845eb660c3875	,
0x131ca0cec87811dfbca65a2fd92a12b53c4f1c49	,
0xddb108734ce45b9eb7cb4afa60585ffa51e8b82f	,
0x8b066d7cda802a9fca96855c9b631b219b667d24	,
0x8d629e615048d65bec146c0b6066ace65428e555	,
0x0e7d4c5770ca2e9f0fdef52c3913f9ab61459090	,
0x3b928e16011c6b17159e70723285b2f1d57c9571	,
0x11f6d2142b438ac6d3c8b3d68d04c4c5a7946aec	,
0x12fdfeda9ed796320bd3c553a8428234aafd1ccb	,
0x0c68be49a2d2828c1a832b9a254809af347224db	,
0x678a472eaaf3676444ae6c80c0c0d0c949ac94bd	,
0xbdf9f39618fdc79c2e58fb3f009cd7fee9b26dc1	,
0x51d2d81273880fadc05cbca3cd45237811fb00ec	,
0x8304269ec873f85b4fa528a58934d845a60d0d9e	,
0x15ce8565a0f20202ef2cf554ca3d271d53d3a314	,
0x2ecad84d089db0e92eb97f8e9c7c2df87c953106	,
0xc7f1602ba4945dbd4d7bd783c1eead384ea372c6
)
order by rank_score desc, amount_usd desc, transaction_count desc
