def _update_from_jde_logic(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Performs an update with JDE-specific conditional logic, reading output
    columns directly from the logic blocks.
    """
    self.logger.info(f"Running JDE logic update from '{config['source_name']}'.")
    lookup_df = self.lookup_tables.get(config['source_name'])
    if lookup_df is None or lookup_df.empty:
        self.logger.warning(f"JDE source '{config['source_name']}' is empty. Skipping.")
        return df
    
    # Filter lookup source by the specified filter
    for col, val in config['filter'].items():
        lookup_df = lookup_df[lookup_df[col] == val]
    
    # Ensure unique join keys in the lookup table
    join_key = config['join_on'][1]
    lookup_df = lookup_df.drop_duplicates(subset=[join_key])

    if lookup_df.empty:
        self.logger.warning(f"No data in JDE source after applying filter: {config['filter']}.")
        return df
        
    df_join_col, source_join_col = config['join_on']
    # Merge only the join key from the main df to avoid column conflicts
    merged = df[[df_join_col]].merge(lookup_df, left_on=df_join_col, right_on=source_join_col, how='left')

    update_mappings = config.get('update_map', {}).copy()

    # Apply conditional format logic if it exists
    if 'format_logic' in config:
        fmt_logic = config['format_logic']
        # I've corrected your typo from "ouput_col" to "output_col"
        output_col = fmt_logic['output_col'] 
        
        format_condition = merged[fmt_logic['condition_col']].isin(fmt_logic['condition_values'])
        
        # Calculate the new format values and store them in a temporary column in `merged`
        # This column is named after the final target column for simplicity.
        merged[output_col] = np.where(
            format_condition,
            merged[fmt_logic['true_col']].str[:2],
            merged[fmt_logic['false_col']]
        )
        # Add this newly calculated column to our mapping
        update_mappings[output_col] = output_col

    # Apply date clamping logic if it exists
    if 'date_logic' in config:
        date_logic = config['date_logic']
        output_col = date_logic['output_col']
        
        min_date = pd.Timestamp(date_logic['min_date'])
        max_date = pd.Timestamp(date_logic['max_date'])
        
        # Calculate the new date values and store them in the target column in `merged`
        merged[output_col] = merged[date_logic['date_col']].clip(lower=min_date, upper=max_date)
        
        # Add this newly calculated column to our mapping
        update_mappings[output_col] = output_col

    # Update the original DataFrame where there were successful matches
    update_mask = merged[source_join_col].notna()
    
    if not update_mask.any():
        self.logger.info("No matching rows found to update from JDE logic.")
        return df

    # A single loop now handles all updates: simple maps + special logic
    for target_col, source_col in update_mappings.items():
            # Use .loc to ensure alignment and avoid SettingWithCopyWarning
            df.loc[update_mask, target_col] = merged.loc[update_mask, source_col].values
            
    return df
