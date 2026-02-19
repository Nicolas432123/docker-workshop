{%macro get_vendor_name(vendorid) -%}
case 
    when {{vendorid}} = 1 then 'Amazon'
    when {{ vendorid}} = 2 then 'Apple'
    when {{vendorid}} = 4 then 'Google'
end 
{%- endmacro %}