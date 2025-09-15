/*Used to create a table in supabase*/

CREATE TABLE IF NOT EXISTS public.dev_grades (
    transaction_id uuid PRIMARY KEY REFERENCES public.transactions(id) ON DELETE CASCADE,

    -- outcome booleans & score
    upsell_possible     boolean,
    upsell_offered      boolean,
    upsize_possible     boolean,
    upsize_offered      boolean,
    score               numeric,

    -- core columns mapped from Step-2
    items_initial               jsonb,
    num_items_initial           integer,
    num_upsell_opportunities    integer,
    items_upsellable            jsonb,
    num_upsell_offers           integer,
    items_upsold                jsonb,
    num_upsell_success          integer,
    num_largest_offers          integer,
    num_upsize_opportunities    integer,
    items_upsizeable            jsonb,
    num_upsize_offers           integer,
    num_upsize_success          integer,
    items_upsize_success        jsonb,
    num_addon_opportunities     integer,
    items_addonable             jsonb,
    num_addon_offers            integer,
    num_addon_success           integer,
    items_addon_success         jsonb,
    items_after                 jsonb,
    num_items_after             integer,
    feedback                    text,
    issues                      text,

    -- flags from Step-1 meta
    complete_order      integer,
    mobile_order        integer,
    coupon_used         integer,
    asked_more_time     integer,
    out_of_stock_items  text,

    -- cost & reasoning metadata
    gpt_price           numeric,
    reasoning_summary   text,
    video_file_path     text,
    video_link          text,

    -- raw transcript and full details blob
    transcript          text,
    details             jsonb,

    created_at          timestamptz DEFAULT now(),
    updated_at          timestamptz DEFAULT now()
);

-- Keep updated_at in sync
CREATE OR REPLACE FUNCTION public.set_updated_at_dev_grades()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tgr_set_updated_at_dev_grades ON public.dev_grades;
CREATE TRIGGER tgr_set_updated_at_dev_grades
BEFORE UPDATE ON public.dev_grades
FOR EACH ROW EXECUTE PROCEDURE public.set_updated_at_dev_grades();
