{% macro extract_indicator_metadata() %}
  {% set result = {
    'indicators': [],
    'codes': [],
    'usage': [],
    'thresholds': []
  } %}
  
  {% if execute %}
    {% for node in graph.nodes.values() %}
      {% if node.resource_type == 'model' %}
        
        {# Process model-level indicators - support both single and multiple #}
        {% set model_meta = node.config.get('meta', {}).get('indicator', {}) %}
        {% set model_indicators = node.config.get('meta', {}).get('indicators', []) %}
        
        {# Handle single indicator #}
        {% if model_meta %}
          
          {# Generate sort order based on type and name #}
          {% set sort_order = '' %}
          {% if model_meta.type == 'CONDITION' %}
            {% set sort_order = 'COND_' ~ (model_meta.clinical_domain|upper)[:6] ~ '_' ~ model_meta.id[5:] %}
          {% elif model_meta.type == 'BRF' %}
            {% if 'BMI' in model_meta.id %}
              {% set sort_order = 'BRF_1_BMI' %}
            {% elif 'SMOKING' in model_meta.id %}
              {% set sort_order = 'BRF_2_SMOKING' %}
            {% elif 'ALCOHOL' in model_meta.id %}
              {% set sort_order = 'BRF_3_ALCOHOL' %}
            {% else %}
              {% set sort_order = 'BRF_9_' ~ model_meta.name_short|upper %}
            {% endif %}
          {% else %}
            {% set sort_order = model_meta.type ~ '_' ~ model_meta.name_short|upper %}
          {% endif %}
          
          {# Add core indicator #}
          {% do result.indicators.append({
            'indicator_id': model_meta.id,
            'indicator_type': model_meta.type,
            'category': model_meta.category,
            'clinical_domain': model_meta.get('clinical_domain'),
            'name_short': model_meta.name_short,
            'description_short': model_meta.description_short,
            'description_long': model_meta.description_long,
            'source_model': node.name|upper,
            'source_column': model_meta.source_column|upper,
            'is_qof': model_meta.get('is_qof', false),
            'qof_indicator': model_meta.get('qof_indicator'),
            'sort_order': model_meta.get('sort_order', sort_order)
          }) %}
          
          {# Add usage contexts #}
          {% for context in model_meta.get('usage_contexts', []) %}
            {% do result.usage.append({
              'indicator_id': model_meta.id,
              'usage_context': context
            }) %}
          {% endfor %}
          
          {# Add code clusters #}
          {% for cluster in model_meta.get('code_clusters', []) %}
            {% do result.codes.append({
              'indicator_id': model_meta.id,
              'cluster_id': cluster.cluster_id,
              'code_category': cluster.category
            }) %}
          {% endfor %}
          
          {# Add thresholds #}
          {% for threshold in model_meta.get('thresholds', []) %}
            {% do result.thresholds.append({
              'indicator_id': model_meta.id,
              'population_group': threshold.population_group,
              'threshold_type': threshold.threshold_type,
              'threshold_value': threshold.threshold_value,
              'threshold_operator': threshold.threshold_operator,
              'threshold_unit': threshold.threshold_unit,
              'description': threshold.description,
              'sort_order': threshold.get('sort_order', loop.index)
            }) %}
          {% endfor %}
          
        {% endif %}
        
        {# Handle multiple indicators (for tables with many conditions) #}
        {% for ind_meta in model_indicators %}
          
          {# Generate sort order based on type and name #}
          {% set sort_order = '' %}
          {% if ind_meta.type == 'CONDITION' %}
            {% set sort_order = 'COND_' ~ (ind_meta.clinical_domain|upper)[:6] ~ '_' ~ ind_meta.id[5:] %}
          {% elif ind_meta.type == 'BRF' or ind_meta.type == 'RISK_FACTOR' %}
            {% if 'BMI' in ind_meta.id %}
              {% set sort_order = 'BRF_1_BMI' %}
            {% elif 'SMOKING' in ind_meta.id %}
              {% set sort_order = 'BRF_2_SMOKING' %}
            {% elif 'ALCOHOL' in ind_meta.id %}
              {% set sort_order = 'BRF_3_ALCOHOL' %}
            {% else %}
              {% set sort_order = 'BRF_9_' ~ ind_meta.name_short|upper %}
            {% endif %}
          {% else %}
            {% set sort_order = ind_meta.type ~ '_' ~ ind_meta.name_short|upper %}
          {% endif %}
          
          {# Add core indicator #}
          {% do result.indicators.append({
            'indicator_id': ind_meta.id,
            'indicator_type': ind_meta.type,
            'category': ind_meta.category,
            'clinical_domain': ind_meta.get('clinical_domain'),
            'name_short': ind_meta.name_short,
            'description_short': ind_meta.description_short,
            'description_long': ind_meta.description_long,
            'source_model': node.name|upper,
            'source_column': ind_meta.source_column|upper,
            'is_qof': ind_meta.get('is_qof', false),
            'qof_indicator': ind_meta.get('qof_indicator'),
            'sort_order': ind_meta.get('sort_order', sort_order)
          }) %}
          
          {# Add usage contexts #}
          {% for context in ind_meta.get('usage_contexts', []) %}
            {% do result.usage.append({
              'indicator_id': ind_meta.id,
              'usage_context': context
            }) %}
          {% endfor %}
          
          {# Add code clusters #}
          {% for cluster in ind_meta.get('code_clusters', []) %}
            {% do result.codes.append({
              'indicator_id': ind_meta.id,
              'cluster_id': cluster.cluster_id,
              'code_category': cluster.category
            }) %}
          {% endfor %}
          
          {# Add thresholds #}
          {% for threshold in ind_meta.get('thresholds', []) %}
            {% do result.thresholds.append({
              'indicator_id': ind_meta.id,
              'population_group': threshold.population_group,
              'threshold_type': threshold.threshold_type,
              'threshold_value': threshold.threshold_value,
              'threshold_operator': threshold.threshold_operator,
              'threshold_unit': threshold.threshold_unit,
              'description': threshold.description,
              'sort_order': threshold.get('sort_order', loop.index)
            }) %}
          {% endfor %}
          
        {% endfor %}
        
        {# Process column-level indicators (for future BRFs) #}
        {% for column in node.columns.values() %}
          {% set col_meta = column.get('meta', {}).get('indicator', {}) %}
          {% if col_meta %}
            
            {# Generate sort order #}
            {% set sort_order = '' %}
            {% if col_meta.type == 'BRF' %}
              {% if 'BMI' in col_meta.id %}
                {% set sort_order = 'BRF_1_BMI' %}
              {% elif 'SMOKING' in col_meta.id %}
                {% set sort_order = 'BRF_2_SMOKING' %}
              {% elif 'ALCOHOL' in col_meta.id %}
                {% set sort_order = 'BRF_3_ALCOHOL' %}
              {% else %}
                {% set sort_order = 'BRF_9_' ~ col_meta.name_short|upper %}
              {% endif %}
            {% else %}
              {% set sort_order = col_meta.type ~ '_' ~ col_meta.name_short|upper %}
            {% endif %}
            
            {# Add core indicator #}
            {% do result.indicators.append({
              'indicator_id': col_meta.id,
              'indicator_type': col_meta.type,
              'category': col_meta.category,
              'clinical_domain': col_meta.get('clinical_domain'),
              'name_short': col_meta.name_short,
              'description_short': col_meta.description_short,
              'description_long': col_meta.description_long,
              'source_model': node.name|upper,
              'source_column': col_meta.source_column|upper,
              'is_qof': col_meta.get('is_qof', false),
              'qof_indicator': col_meta.get('qof_indicator'),
              'sort_order': col_meta.get('sort_order', sort_order)
            }) %}
            
            {# Add usage contexts #}
            {% for context in col_meta.get('usage_contexts', []) %}
              {% do result.usage.append({
                'indicator_id': col_meta.id,
                'usage_context': context
              }) %}
            {% endfor %}
            
            {# Add code clusters #}
            {% for cluster in col_meta.get('code_clusters', []) %}
              {% do result.codes.append({
                'indicator_id': col_meta.id,
                'cluster_id': cluster.cluster_id,
                'code_category': cluster.category
              }) %}
            {% endfor %}
            
            {# Add thresholds #}
            {% for threshold in col_meta.get('thresholds', []) %}
              {% do result.thresholds.append({
                'indicator_id': col_meta.id,
                'population_group': threshold.population_group,
                'threshold_type': threshold.threshold_type,
                'threshold_value': threshold.threshold_value,
                'threshold_operator': threshold.threshold_operator,
                'threshold_unit': threshold.threshold_unit,
                'description': threshold.description,
                'sort_order': threshold.get('sort_order', loop.index)
              }) %}
            {% endfor %}
            
          {% endif %}
        {% endfor %}
        
      {% endif %}
    {% endfor %}
  {% endif %}
  
  {{ return(result) }}
{% endmacro %}