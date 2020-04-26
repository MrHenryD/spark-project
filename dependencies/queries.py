class BatchQueries:
	suspicious_purchases = """
	select *
    from
    (
        select *, suspicious_purchases / purchases as suspicious_ratio
        from
        (
            select 
                user, 
                sum(price) as total_spending,
                count(*) as purchases,
                sum(case when suspicious=0 then 1 else 0 end) as suspicious_purchases
            from tmp
            group by user
        )
    )
    where suspicious_ratio > 0.5 and suspicious_purchases > 3
	"""

class StreamingQueries:
    pass
	