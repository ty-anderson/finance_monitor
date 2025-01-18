from jobs.fed_funds_rate import fed_funds_etl, report


if __name__ == '__main__':
    fed_funds_etl.main()
    report.report_fed_funds_rate()
