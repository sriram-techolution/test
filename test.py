def process_validation_results(validation_dict, df):
    """
    Process validation results dictionary and add ValidFlag column to dataframe
    
    Args:
        validation_dict: Dict with column names as keys, each containing:
                        {'failed_rows': [list of row indices], 'err_message': [error message]}
        df: Pandas DataFrame to add ValidFlag column to
    
    Returns:
        tuple: (updated_dataframe, failed_rows_summary)
    """
    
    # Dictionary to store row numbers and their error messages
    row_errors = defaultdict(list)
    all_failed_rows = set()
    
    # Process each column's validation results
    for column_name, validation_result in validation_dict.items():
        failed_rows = validation_result.get('failed_rows', [])
        err_message = validation_result.get('err_message', [''])[0]  # Get first error message
        
        # Add failed rows to our tracking
        for row_num in failed_rows:
            all_failed_rows.add(row_num)
            row_errors[row_num].append(f"{column_name}: {err_message}")
    
    
    for row_num in sorted(all_failed_rows):
        combined_errors = " | ".join(row_errors[row_num])
        # logger.error(f"Row {row_num} failed validation: {combined_errors}")
    
    # Add ValidFlag column to dataframe
    # Assuming row numbers are 0-based indices
    df['ValidFlag'] = 'Y'  # Default to valid
    
    # Set ValidFlag to 'N' for failed rows
    if all_failed_rows:
        # Convert to list and ensure they're within dataframe bounds
        failed_indices = [idx for idx in all_failed_rows if idx < len(df)]
        df.loc[failed_indices, 'ValidFlag'] = 'N'
    
    # Create summary of failed rows
    failed_rows_summary = {}
    for row_num in sorted(all_failed_rows):
        failed_rows_summary[row_num] = {
            'error_messages': row_errors[row_num],
            'combined_error': " | ".join(row_errors[row_num])
        }
    
    return df, failed_rows_summary
