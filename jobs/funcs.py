from io import BytesIO
import pandas as pd
import matplotlib.pyplot as plt


def format_table_with_pipes(df):
    # Calculate the max width for each column
    col_widths = {col: max(df[col].apply(len).max(), len(col)) for col in df.columns}

    # Create the header row
    header = ' | '.join([f'{col.ljust(col_widths[col])}' for col in df.columns])
    separator = '|'.join(['-' * (col_widths[col] + 2) for col in df.columns])

    # Create the data rows
    rows = []
    for _, row in df.iterrows():
        formatted_row = ' | '.join([f'{str(row[col]).ljust(col_widths[col])}' for col in df.columns])
        rows.append(formatted_row)

    # Combine header, separator, and rows into the final table
    table = f'| {header} |\n|{separator}|\n' + '\n'.join([f'| {row} |' for row in rows])
    return table



def plot_df_data(df) -> BytesIO:
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    # Plot the data
    ax = df.plot(y='value', kind='line', marker='o')

    # Add data labels
    for i, value in enumerate(df['value']):
        ax.annotate(f'{value}%', (df.index[i], value), textcoords="offset points", xytext=(0, 5), ha='center')

    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    return buffer
