{#- Why are customers unhappy, and is it getting better? Reviews by month x topic x sentiment, from
    the pinned AI labels (Session 22).

    Grain = REVIEW (one row of fct_reviews), not text: two reviews with the same words count twice,
    because two customers said it. A review with no message is 'no text'; one whose text has no
    pinned answer yet is 'not labelled'; an answer outside the label set is 'unreadable answer' —
    kept visible so a report never hides the share the AI did not cover. -#}

select
    date_trunc(month, r.review_created_at)::date                          as review_month,
    case
        when not r.has_message      then 'no text'
        when l.message_hash is null then 'not labelled'
        when not l.label_ok         then 'unreadable answer'
        else l.topic
    end                                                                   as topic,
    case when r.has_message and l.label_ok then l.sentiment end           as sentiment,
    count(*)                                                              as reviews,
    sum(r.review_score)                                                   as score_sum,
    count_if(r.review_score <= 2)                                         as low_score_reviews
from {{ ref('fct_reviews') }} as r
left join {{ ref('review_labels') }} as l
    on l.message_hash = r.message_hash
group by 1, 2, 3
