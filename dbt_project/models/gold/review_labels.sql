{#- The AI's sentiment and topic for each distinct review text, under the ONE pinned model and prompt
    version (vars ai_label_model / ai_label_prompt_version; Session 22). Keyed by message_hash, the
    SHA-256 of the trimmed message — the same value as fct_reviews.message_hash, computed on
    Databricks; test review_labels_join_back proves the two platforms agree.

    A text not answered yet has no row: a report shows it as "not labelled", never as a guess.
    label_ok = false: the model answered outside the label set; sentiment and topic are NULL. -#}

with current_ as (
    {{ current_state('stg_review_labels', ['input_hash', 'model', 'prompt_version'], [
        'input_hash', 'model', 'prompt_version', 'sentiment', 'topic', 'label_ok', 'called_at',
    ]) }}
)

select
    input_hash   as message_hash,
    sentiment,
    topic,
    label_ok,
    model,
    prompt_version,
    called_at    as labelled_at,
    silver_version
from current_
where model = '{{ var("ai_label_model") }}'
  and prompt_version = '{{ var("ai_label_prompt_version") }}'
