CREATE TABLE IF NOT EXISTS training.loans.issued_loans_silver
SELECT
  id AS loan_id,
  loan_amnt,
  CAST(regexp_extract(term, '(\\d+)') AS INT) AS term,
  CAST(regexp_extract(int_rate, '(.+)%') AS DOUBLE) AS int_rate,
  installment,
  grade,
  sub_grade,
  CASE
    WHEN substr(emp_length, 1, 1) = '<' THEN 0 -- <1 year should count as 0
    ELSE regexp_extract(emp_length, '(\\d+)+')
  END AS emp_length,
  home_ownership,
  annual_inc,
  verification_status,
  to_date(issue_d, 'MMM-yy') as issue_date,
  EXTRACT(
    YEAR
    FROM
      issue_date
  ) as issue_year,
  EXTRACT(
    MONTH
    FROM
      issue_date
  ) as issue_month,
  purpose,
  zip_code,
  addr_state,
  dti,
  delinq_2yrs,
  to_date(earliest_cr_line, 'MMM-yy') as earliest_cr_line,
  fico_range_low,
  fico_range_high,
  inq_last_6mths,
  mths_since_last_delinq,
  mths_since_last_record,
  open_acc,
  pub_rec,
  revol_bal,
  revol_util,
  total_acc,
  acc_open_past_24mths,
  mths_since_recent_bc,
  bc_util
FROM
  main.anicholls.issued_loans_bronze;

SELECT *
FROM main.anicholls.issued_loans_silver
LIMIT 100