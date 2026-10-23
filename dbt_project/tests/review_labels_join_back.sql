{#- The AI labels, hashed on Databricks, join the reviews, hashed on Snowflake (Session 22).

    Both platforms trim the message and take its SHA-256; if their white space or hashing differed
    (Snowflake's \s is not Java's — constraints.md S22), labels would silently join nothing and the
    complaint trends would show "not labelled" where an answer exists. Every pinned label's hash must
    match at least one review in fct_reviews. One row per label that matches none — empty = pass. -#}

select l.message_hash, l.sentiment, l.topic
from {{ ref('review_labels') }} as l
left join (select distinct message_hash from {{ ref('fct_reviews') }}) as r
    on r.message_hash = l.message_hash
where r.message_hash is null
