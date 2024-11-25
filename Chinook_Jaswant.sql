-- to use the chinook database
use chinook;

-- Q1. Does any table have missing values or duplicates? If yes how would you handle it ?
-- checking rows with null values in album table
SELECT * FROM album
WHERE
	album_id IS NULL OR
    title IS NULL OR
    artist_id IS NULL;
-- no null values

-- checking rows with null values in artist table
SELECT * FROM artist
WHERE 
	name IS NULL OR
	artist_id IS NULL;
-- no null values

-- checking rows with null values in customer table
SELECT * 
FROM customer 
WHERE customer_id IS NULL
	OR first_name IS NULL 
	OR last_name IS NULL 
	OR company IS NULL 
	OR address IS NULL 
	OR city IS NULL 
	OR state IS NULL 
	OR country IS NULL 
	OR postal_code IS NULL 
	OR phone IS NULL 
	OR fax IS NULL 
	OR email IS NULL 
	OR support_rep_id IS NULL;
-- we can see that there are null values in
-- company, state, postal_code, phone, fax

-- checking rows with null values in employee table
SELECT * FROM employee
WHERE employee_id IS NULL
	OR first_name IS NULL 
	OR title IS NULL 
	OR reports_to IS NULL 
	OR birthdate IS NULL 
	OR hire_date IS NULL 
	OR address IS NULL 
	OR city IS NULL 
	OR state IS NULL 
	OR country IS NULL 
	OR postal_code IS NULL 
	OR phone IS NULL 
	OR fax IS NULL 
	OR email IS NULL;

-- there is a null value in report_to column

-- checking rows with null values in genre table
SELECT * FROM genre
WHERE genre_id IS NULL OR name IS NULL;
-- No NUll value

-- checking rows with null values in invoice table
SELECT * FROM invoice
WHERE invoice_id IS NULL 
	OR customer_id IS NULL
    OR invoice_date IS NULL
    OR billing_address IS NULL
    OR billing_city IS NULL
    OR billing_state IS NULL
    OR billing_country IS NULL
    OR billing_postal_code IS NULL
    OR total IS NULL;
-- no null values

-- checking rows with null values in invoice_line table
SELECT * FROM invoice_line
WHERE invoice_id IS NULL
	OR track_id IS NULL
    OR unit_price IS NULL
    OR quantity IS NULL
    OR invoice_line_id IS NULL;
-- no null values

-- checking rows with null values in media_type table
SELECT * FROM media_type
WHERE name IS NULL OR media_type_id IS NULL;
-- no NULL values

-- checking rows with null values in playlist table
SELECT * FROM playlist
WHERE playlist_id IS NULL OR name IS NULL;
-- no NULL values

-- checking rows with null values in playlist_track table
SELECT * FROM playlist_track
WHERE playlist_id IS NULL OR track_id IS NULL;
-- NO NULL VALUES

SELECT * FROM track
WHERE track_id IS NULL OR
	name IS NULl OR
    album_id IS NULL OR
    media_type_id IS NULL OR
    genre_id IS NULL OR
    composer IS NULL OR
    milliseconds IS NULL OR
    bytes IS NULL OR
    unit_price IS NULL;
-- THERE IS ONE COLUMN which has null values
-- which is composer

-- Q2.Find the top-selling tracks and top artist in the USA and identify their most famous genres. 

-- top 10 tracks
WITH TopTracksUSA AS (
	SELECT
		t.track_id,
		t.name AS track_name,
		g.name AS genre_name,
		ar.name AS artist_name,
		SUM(il.unit_price * il.quantity) AS sales
	FROM
		track t
	JOIN
		genre g ON t.genre_id = g.genre_id
	JOIN
		album al ON t.album_id = al.album_id
	JOIN
		artist ar ON al.artist_id = ar.artist_id
	JOIN
		invoice_line il ON il.track_id = t.track_id
	JOIN
		invoice i ON il.invoice_id = i.invoice_id
	WHERE
		i.billing_country = 'USA'
	GROUP BY
		t.track_id,t.name, g.name, ar.name
	ORDER BY 
		sales DESC
	LIMIT 10
),
TopArtistUSA AS (
	SELECT
		ar.artist_id,
		ar.name as artist_name,
        SUM(il.unit_price * il.quantity) AS sales
	FROM
		artist ar
	JOIN
		album al ON al.artist_id = ar.artist_id
	JOIN
		track t ON t.album_id  = al.album_id
	JOIN
		invoice_line il ON il.track_id = t.track_id
	JOIN
		invoice i ON i.invoice_id = il.invoice_id
	WHERE
		i.billing_country = 'USA'
	GROUP BY
		ar.artist_id,ar.name
	ORDER BY
		sales DESC
	LIMIT 1
),
TopUSAArtistGenres AS (
	SELECT
		DISTINCT g.name as genre_name
	FROM
		genre g
	JOIN
		track t ON g.genre_id = t.genre_id
	JOIN
		album al ON al.album_id = t.album_id
	WHERE
		al.artist_id = (SELECT artist_id FROM TopArtistUSA)
)

-- SELECT * FROM TopTracksUSA;
-- SELECT * FROM TopArtistUSA;
-- SELECT * FROM TopUSAArtistGenres;
SELECT DISTINCT genre_name FROM (
	SELECT genre_name FROM TopTracksUSA
	UNION ALL
	SELECT genre_name FROM TopUSAArtistGenres
) temp;

-- q3What is the customer demographic breakdown (age, gender, location) of Chinook's customer base?

SELECT 
    c.country,
    coalesce(c.state,'N/A') AS state,
    c.city,
    COUNT(c.customer_id) AS customer_count
FROM 
    customer c
GROUP BY 
    c.country, c.state, c.city
ORDER BY 
    customer_count DESC;

SELECT 
    c.country,
    COUNT(c.customer_id) AS customer_count
FROM 
    customer c
GROUP BY 
    c.country
ORDER BY 
    customer_count DESC;

-- Q4 : Calculate the total revenue and number of invoices for each country, state, and city

SELECT
	billing_country as country,
    billing_state as state,
    billing_city as city,
    COUNT(invoice_id) as Number_of_invoices,
    SUM(total) as Total_Revenue
FROM
	invoice
GROUP BY
	billing_country,billing_state,billing_city
ORDER BY
	Total_Revenue DESC, Number_of_invoices DESC;
    
-- Q5:) Find the top 5 customers by total revenue in each country

WITH CustomerRevenueRanked AS (
    SELECT
        c.customer_id,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_full_name,
        c.country,
        SUM(i.total) AS total_revenue,
        RANK() OVER(PARTITION BY c.country ORDER BY SUM(i.total) DESC) AS rank_num
    FROM
        customer c
    JOIN
        invoice i ON i.customer_id = c.customer_id
    GROUP BY
        c.customer_id, customer_full_name, c.country
)
SELECT
    customer_id,
    customer_full_name,
    country,
    total_revenue,
    rank_num
FROM
    CustomerRevenueRanked
WHERE
    rank_num <= 5
ORDER BY
    country, rank_num;
    
    
-- Q6)Identify the top-selling track for each customer

WITH CustomerTrackSales AS (
    SELECT
        c.customer_id,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_full_name,
        t.name AS track_name,
        SUM(il.unit_price * il.quantity) AS total_revenue,
        ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY SUM(il.unit_price * il.quantity) DESC) AS rank_num
    FROM
        customer c
    JOIN
        invoice i ON c.customer_id = i.customer_id
    JOIN
        invoice_line il ON i.invoice_id = il.invoice_id
    JOIN
        track t ON il.track_id = t.track_id
    GROUP BY
        c.customer_id, customer_full_name, t.name
)
SELECT
    customer_id,
    customer_full_name,
    track_name,
    total_revenue
FROM
    CustomerTrackSales
WHERE
    rank_num = 1
ORDER BY
    customer_id;
    
-- Q7) Are there any patterns or trends in customer purchasing behavior (e.g., frequency of purchases, preferred payment methods, average order value)?

WITH InvoiceDates AS (
    SELECT
        c.customer_id,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        i.invoice_date,
        DATEDIFF(
            LEAD(i.invoice_date) OVER (PARTITION BY c.customer_id ORDER BY i.invoice_date),
            i.invoice_date
        ) AS days_between_purchases
    FROM
        customer c
    JOIN
        invoice i ON c.customer_id = i.customer_id
),
PurchaseFrequency AS (
    SELECT
        customer_id,
        customer_name,
        COUNT(*) AS total_purchases,
        AVG(days_between_purchases) AS avg_days_between_purchases
    FROM
        InvoiceDates
    GROUP BY
        customer_id, customer_name
),
CustomerSpending AS (
    SELECT
        c.customer_id,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_full_name,
        COUNT(i.invoice_id) AS total_purchases,
        SUM(i.total) AS total_spent,
        AVG(i.total) AS avg_order_value
    FROM
        customer c
    JOIN
        invoice i ON c.customer_id = i.customer_id
    GROUP BY
        c.customer_id, customer_full_name
)
SELECT
    f.customer_id,
    f.customer_name,
    f.total_purchases,
    f.avg_days_between_purchases,
    s.total_spent,
    s.avg_order_value
FROM
    PurchaseFrequency f
JOIN
    CustomerSpending s ON f.customer_id = s.customer_id
ORDER BY
    f.avg_days_between_purchases, f.total_purchases DESC;
    
-- Q8) What is the customer churn rate?

WITH MostRecentInvoice AS (
    -- Find the most recent invoice date
    SELECT MAX(invoice_date) AS most_recent_invoice_date
    FROM invoice
),
CutoffDate AS (
    -- Calculate the cutoff date as 1 year before the most recent invoice date
    SELECT DATE_SUB(most_recent_invoice_date, INTERVAL 1 YEAR) AS cutoff_date
    FROM MostRecentInvoice
),
ChurnedCustomers AS (
    -- Find customers who haven't made a purchase in over a year or never made a purchase
    SELECT 
        c.customer_id,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_full_name,
        MAX(i.invoice_date) AS last_purchase_date
    FROM 
        customer c
    LEFT JOIN invoice i ON c.customer_id = i.customer_id
    GROUP BY 
        c.customer_id, customer_full_name
    HAVING 
        MAX(i.invoice_date) IS NULL OR MAX(i.invoice_date) < (SELECT cutoff_date FROM CutoffDate)
)
-- Calculate the churn rate
SELECT 
    COUNT(*) AS churned_customers,
    (SELECT COUNT(*) FROM customer) AS total_customers,
    ROUND((COUNT(*) / (SELECT COUNT(*) FROM customer)) * 100, 2) AS churn_rate
FROM 
    ChurnedCustomers;
    
-- Q9) Calculate the percentage of total sales contributed by each genre in the USA and identify the best-selling genres and artists.

WITH TotalTrackSalesInUSA AS (
	SELECT
		SUM(quantity) as total_sales
	FROM
		invoice_line il
	JOIN
		invoice i ON i.invoice_id = il.invoice_id
	WHERE
		billing_country = 'USA'
)
SELECT
	g.name as genre_name,
    SUM(il.quantity) as total_genre_sales,
	(SUM(il.quantity)/(SELECT total_sales FROM TotalTrackSalesInUSA) * 100) as percentage_of_sales_contributed
FROM
	genre g
JOIN
	track t ON g.genre_id = t.genre_id
JOIN
	invoice_line il ON il.track_id = t.track_id
JOIN
	invoice i ON i.invoice_id  = il.invoice_id
WHERE
	i.billing_country = 'USA'
GROUP BY
	genre_name
ORDER BY
	percentage_of_sales_contributed DESC;

WITH TotalTrackSalesInUSA AS (
	SELECT
		SUM(il.quantity) AS total_sales
	FROM
		invoice_line il
	JOIN
		invoice i ON i.invoice_id = il.invoice_id
	WHERE
		i.billing_country = 'USA'
)
SELECT
	a.artist_id,
	a.name AS artist_name,
    SUM(il.quantity) AS total_artist_sales,
    ROUND((SUM(il.quantity) / (SELECT total_sales FROM TotalTrackSalesInUSA)) * 100, 2) AS percentage_of_sales_contributed
FROM
	artist a
JOIN
	album ab ON a.artist_id = ab.artist_id
JOIN
	track t ON ab.album_id = t.album_id
JOIN
	invoice_line il ON il.track_id = t.track_id
JOIN
	invoice i ON i.invoice_id = il.invoice_id
WHERE
	i.billing_country = 'USA'
GROUP BY
	a.artist_id,a.name
ORDER BY
	percentage_of_sales_contributed DESC;
    
-- q10) Find customers who have purchased tracks from at least 3 different+ genres
SELECT
	c.customer_id,
    CONCAT(c.first_name,' ',c.last_name) as customer_name,
    COUNT(DISTINCT t.genre_id) as total_different_genres
FROM
	customer c
JOIN 
	invoice i ON c.customer_id = i.customer_id
JOIN
	invoice_line il ON i.invoice_id = il.invoice_id
JOIN 
	track t ON t.track_id = il.track_id
GROUP BY
	c.customer_id, customer_name
HAVING
	COUNT(DISTINCT t.genre_id) >= 3
ORDER BY
	total_different_genres DESC;

-- Q11) Rank genres based on their sales performance in the USA

WITH genreSalesInUsa AS (
	SELECT
		g.genre_id,
		g.name as genre_name,
		SUM(il.unit_price * quantity) as total_sales
	FROM
		genre g
	JOIN 
		track t ON g.genre_id = t.genre_id
	JOIN
		invoice_line il ON t.track_id = il.track_id
	JOIN
		invoice i ON i.invoice_id = il.invoice_id
	JOIN
		album al on al.album_id = t.album_id
	JOIN
		artist a ON a.artist_id = al.artist_id
	WHERE
		i.billing_country = 'USA'
	GROUP BY
		g.genre_id, genre_name
)
SELECT 
	genre_id,
    genre_name,
    total_sales,
    RANK() OVER(ORDER BY total_sales DESC) as genre_rank
FROM
	genreSalesInUsa
ORDER BY
	genre_rank;
    
-- Q12) Identify customers who have not made a purchase in the last 3 months

SELECT
	c.customer_id,
    CONCAT(c.first_name,' ',c.last_name) as customer_name
FROM
	customer c
JOIN
	invoice i ON c.customer_id = i.customer_id
GROUP BY
	c.customer_id,customer_name
HAVING
	MAX(i.invoice_date) <= DATE_SUB(current_timestamp(), INTERVAL 3 MONTH)
ORDER BY
	customer_name, customer_id;
    
    
-- subjective question

-- q1) Recommend the three albums from the new record label that should be prioritised for advertising and promotion in the USA based on genre sales analysis

WITH TopGenreSalesInUsa AS (
	SELECT 
		g.genre_id,
        g.name as genre_name,
        SUM(il.unit_price * il.quantity) as total_sales,
        RANK() OVER(ORDER BY SUM(il.unit_price * il.quantity) DESC) as genre_rank
	FROM
		genre g 
	JOIN
		track t on t.genre_id = g.genre_id
	JOIN
		invoice_line il ON il.track_id = t.track_id
	JOIN
		invoice i ON i.invoice_id = il.invoice_id
	WHERE
		i.billing_country = 'USA'
	GROUP BY
		g.genre_id, g.name
	ORDER BY
		total_sales DESC
)
SELECT 
	al.title as album_name,
    a.name as artist_name,
    g.name as genre_name,
    SUM(il.unit_price * il.quantity) as total_sales
FROM
	album al 
JOIN
	track t ON al.album_id = t.album_id
JOIN
	invoice_line il ON il.track_id = t.track_id
JOIN
	genre g ON t.genre_id = g.genre_id
JOIN
	artist a ON al.artist_id = a.artist_id
WHERE
	t.genre_id IN (SELECT genre_id FROM TopGenreSalesInUsa WHERE genre_rank < 3)
GROUP BY
	al.title, g.name, a.name
ORDER BY
	total_sales DESC;

-- Q2)Determine the top-selling genres in countries other than the USA and identify any commonalities or differences.

WITH GenreSalesByCountry AS (
    SELECT
        i.billing_country AS country,
        g.name AS genre_name,
        SUM(il.quantity) AS total_sales,
        RANK() OVER (PARTITION BY i.billing_country ORDER BY SUM(il.quantity) DESC) AS genre_rank
    FROM
        invoice i
    JOIN
        invoice_line il ON i.invoice_id = il.invoice_id
    JOIN
        track t ON il.track_id = t.track_id
    JOIN
        genre g ON t.genre_id = g.genre_id
    WHERE
        i.billing_country <> 'USA'  -- Exclude the USA
    GROUP BY
        i.billing_country, g.name
)
SELECT
    country,
    genre_name,
    total_sales
FROM
    GenreSalesByCountry
WHERE
    genre_rank < 3  -- Top genre for each country
ORDER BY
    total_sales DESC;
    
-- Q3) Customer Purchasing Behavior Analysis: How do the purchasing habits (frequency, basket size, spending amount) of long-term customers differ from those of new customers? What insights can these patterns provide about customer loyalty and retention strategies?

WITH CustomerClassification AS (
    SELECT 
        c.customer_id,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        MIN(i.invoice_date) AS first_purchase_date,
        CASE 
            WHEN MIN(i.invoice_date) < DATE_SUB(CURDATE(), INTERVAL 1 YEAR) THEN 'Long-term'
            ELSE 'New'
        END AS customer_type
    FROM 
        customer c
    JOIN 
        invoice i ON c.customer_id = i.customer_id
    GROUP BY 
        c.customer_id, customer_name
),
CustomerBehavior AS (
    SELECT 
        cc.customer_id,
        cc.customer_name,
        cc.customer_type,
        COUNT(i.invoice_id) AS total_purchases,
        AVG(il.quantity) AS avg_basket_size,
        AVG(i.total) AS avg_spending
    FROM 
        CustomerClassification cc
    JOIN 
        invoice i ON cc.customer_id = i.customer_id
    JOIN 
        invoice_line il ON i.invoice_id = il.invoice_id
    GROUP BY 
        cc.customer_id, cc.customer_name, cc.customer_type
)
-- Compare purchasing habits between long-term and new customers
SELECT 
    customer_type,
    AVG(total_purchases) AS avg_total_purchases,
    AVG(avg_basket_size) AS avg_basket_size,
    AVG(avg_spending) AS avg_spending
FROM 
    CustomerBehavior
GROUP BY 
    customer_type;
    
-- q4)Product Affinity Analysis: Which music genres, artists, or albums are frequently purchased together by customers? 
-- How can this information guide product recommendations and cross-selling initiatives?

WITH GenreCoPurchase AS (
    -- Find all combinations of genres purchased in the same invoice
    SELECT
        il1.invoice_id,
        g1.name AS genre_1,
        g2.name AS genre_2,
        COUNT(*) AS co_purchase_count
    FROM
        invoice_line il1
    JOIN
        track t1 ON il1.track_id = t1.track_id
    JOIN
        genre g1 ON t1.genre_id = g1.genre_id
    JOIN
        invoice_line il2 ON il1.invoice_id = il2.invoice_id
    JOIN
        track t2 ON il2.track_id = t2.track_id
    JOIN
        genre g2 ON t2.genre_id = g2.genre_id
    WHERE
        il1.track_id <> il2.track_id AND g1.genre_id <> g2.genre_id
    GROUP BY
        g1.name, g2.name, il1.invoice_id
)
SELECT
    genre_1,
    genre_2,
    SUM(co_purchase_count) AS total_co_purchases
FROM
    GenreCoPurchase
GROUP BY
    genre_1, genre_2
ORDER BY
    total_co_purchases DESC;
    
-- artist copurchase
WITH ArtistCoPurchase AS (
    -- Find all combinations of artists purchased in the same invoice
    SELECT
        il1.invoice_id,
        a1.name AS artist_1,
        a2.name AS artist_2,
        COUNT(*) AS co_purchase_count
    FROM
        invoice_line il1
    JOIN
        track t1 ON il1.track_id = t1.track_id
    JOIN
        album al1 ON t1.album_id = al1.album_id
    JOIN
        artist a1 ON al1.artist_id = a1.artist_id
    JOIN
        invoice_line il2 ON il1.invoice_id = il2.invoice_id
    JOIN
        track t2 ON il2.track_id = t2.track_id
    JOIN
        album al2 ON t2.album_id = al2.album_id
    JOIN
        artist a2 ON al2.artist_id = a2.artist_id
    WHERE
        il1.track_id <> il2.track_id AND a1.artist_id <> a2.artist_id 
    GROUP BY
        a1.name, a2.name, il1.invoice_id
)
SELECT
    artist_1,
    artist_2,
    SUM(co_purchase_count) AS total_co_purchases
FROM
    ArtistCoPurchase
GROUP BY
    artist_1, artist_2
ORDER BY
    total_co_purchases DESC;
    
-- album purchased together

WITH AlbumCoPurchase AS (
    -- Find all combinations of artists purchased in the same invoice
    SELECT
        il1.invoice_id,
        al1.title AS album_1,
        al2.title AS album_2,
        COUNT(*) AS co_purchase_count
    FROM
        invoice_line il1
    JOIN
        track t1 ON il1.track_id = t1.track_id
    JOIN
        album al1 ON t1.album_id = al1.album_id
    JOIN
        invoice_line il2 ON il1.invoice_id = il2.invoice_id
    JOIN
        track t2 ON il2.track_id = t2.track_id
    JOIN
        album al2 ON t2.album_id = al2.album_id
    WHERE
        il1.track_id <> il2.track_id AND al1.album_id <> al2.album_id 
    GROUP BY
        al1.title, al2.title, il1.invoice_id
)
SELECT
    album_1,
    album_2,
    SUM(co_purchase_count) AS total_co_purchases
FROM
    AlbumCoPurchase
GROUP BY
    album_1, album_2
ORDER BY
    total_co_purchases DESC;

-- Q5) Regional Market Analysis: Do customer purchasing behaviors and churn rates vary across different geographic regions or store locations?
-- How might these correlate with local demographic or economic factors?

-- customer purchasing behaviour per billing country
WITH RegionCustomerBehavior AS (
    SELECT
        i.billing_country AS region,
        COUNT(DISTINCT i.customer_id) AS total_customers,
        COUNT(i.invoice_id) AS total_purchases,
        AVG(il.quantity) AS avg_basket_size,
        AVG(i.total) AS avg_spending
    FROM 
        invoice i
    JOIN 
        invoice_line il ON i.invoice_id = il.invoice_id
    GROUP BY 
        i.billing_country
)
SELECT
    region,
    total_customers,
    total_purchases,
    avg_basket_size,
    avg_spending
FROM
    RegionCustomerBehavior
ORDER BY
    avg_spending DESC;
    
-- churn rate
WITH ChurnedCustomersByRegion AS (
    SELECT 
        c.customer_id,
        i.billing_country AS region,
        MAX(i.invoice_date) AS last_purchase_date,
        CASE 
            WHEN MAX(i.invoice_date) < DATE_SUB(CURDATE(), INTERVAL 1 YEAR) THEN 1
            ELSE 0
        END AS is_churned
    FROM 
        customer c
    LEFT JOIN 
        invoice i ON c.customer_id = i.customer_id
    GROUP BY 
        c.customer_id, region
),
ChurnRateByRegion AS (
    SELECT 
        region,
        COUNT(*) AS total_customers,
        SUM(is_churned) AS churned_customers,
        (SUM(is_churned) / COUNT(*)) * 100 AS churn_rate
    FROM 
        ChurnedCustomersByRegion
    GROUP BY 
        region
)
SELECT 
    region,
    total_customers,
    churned_customers,
    churn_rate
FROM 
    ChurnRateByRegion
ORDER BY 
    churn_rate DESC;
    
    
-- Q6)Customer Risk Profiling: Based on customer profiles (age, gender, location, purchase history), 
-- which customer segments are more likely to churn or pose a higher risk of reduced spending? 
-- What factors contribute to this risk?

WITH CustomerBehavior AS (
    SELECT
        c.customer_id,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        c.country AS customer_location,
        COUNT(i.invoice_id) AS total_purchases,
        SUM(i.total) AS total_spending,
        AVG(i.total) AS avg_spending_per_order,
        MAX(i.invoice_date) AS last_purchase_date,
        DATEDIFF(CURDATE(), MAX(i.invoice_date)) AS days_since_last_purchase
    FROM 
        customer c
    JOIN 
        invoice i ON c.customer_id = i.customer_id
    GROUP BY 
        c.customer_id, customer_name, customer_location
),
CustomerRiskProfile AS (
    SELECT
        customer_id,
        customer_name,
        customer_location,
        total_purchases,
        total_spending,
        avg_spending_per_order,
        last_purchase_date,
        days_since_last_purchase,
        CASE 
            WHEN days_since_last_purchase > 365 THEN 'High Risk'  -- Customer hasn't purchased in over 12 months
            WHEN total_purchases < 3 THEN 'Medium Risk'  -- Customer has made fewer than 3 purchases
            ELSE 'Low Risk'  -- Active and frequent customers
        END AS risk_category
    FROM 
        CustomerBehavior
)
SELECT 
    customer_id,
    customer_name,
    customer_location,
    total_purchases,
    total_spending,
    avg_spending_per_order,
    last_purchase_date,
    days_since_last_purchase,
    risk_category
FROM 
    CustomerRiskProfile
ORDER BY 
    risk_category DESC, days_since_last_purchase DESC;
    
WITH CustomerRiskProfileByDemographics AS (
    SELECT
        c.customer_id,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        c.country AS customer_location,
        SUM(i.total) AS total_spending,
        MAX(i.invoice_date) AS last_purchase_date,
        DATEDIFF(CURDATE(), MAX(i.invoice_date)) AS days_since_last_purchase,
        COUNT(i.invoice_id) AS total_purchases,
        CASE 
            WHEN DATEDIFF(CURDATE(), MAX(i.invoice_date)) > 365 THEN 'High Risk'
            WHEN COUNT(i.invoice_id) < 3 THEN 'Medium Risk'
            ELSE 'Low Risk'
        END AS risk_category
    FROM 
        customer c
    JOIN 
        invoice i ON c.customer_id = i.customer_id
    GROUP BY 
        c.customer_id, customer_name, customer_location
)
SELECT 
    customer_location,
    risk_category,
    COUNT(customer_id) AS customer_count,
    AVG(total_spending) AS avg_spending_per_customer
FROM 
    CustomerRiskProfileByDemographics
GROUP BY 
    customer_location, risk_category
ORDER BY 
    customer_count DESC;


-- Q7) Customer Lifetime Value Modeling: How can you leverage customer data (tenure, purchase history, engagement) to predict the lifetime value of different customer segments? 
-- This could inform targeted marketing and loyalty program strategies. 
-- Can you observe any common characteristics or purchase patterns among customers who have stopped purchasing?

WITH CustomerPurchaseHistory AS (
	SELECT
		c.customer_id,
        CONCAT(c.first_name,' ',c.last_name) as customer_name,
        c.country,
        COALESCE(c.state,'N.A') AS state,
        c.city,
        MIN(i.invoice_date) AS first_purchase_date,
        MAX(i.invoice_date) AS last_purchase_date,
        DATEDIFF(MAX(i.invoice_date), MIN(i.invoice_date)) AS tenure_days,
        COUNT(i.invoice_date) AS total_purchases,
        SUM(i.total) AS total_spent,
        AVG(i.total) AS avg_order_value,
        DATEDIFF(curdate(), MAX(i.invoice_date)) AS days_since_last_purchase
	FROM
		customer c
	JOIN
		invoice i ON i.customer_id = c.customer_id
	GROUP BY
		c.customer_id, customer_name
),
customer_life_time_analysis AS (
	SELECT
		customer_id,
        customer_name,
        country,
        state,
        city,
        tenure_days,
        total_purchases,
        total_spent,
        avg_order_value,
        CASE 
            WHEN tenure_days >= 365 THEN 'Long-Term'
            ELSE 'Short-Term'
        END AS customer_segment,
        CASE 
            WHEN last_purchase_date < DATE_SUB(CURDATE(), INTERVAL 1 YEAR) THEN 'Churned'
            ELSE 'Active'
        END AS customer_status,
        (total_spent / GREATEST(tenure_days, 1)) * 365 AS predicted_annual_value,
        total_spent AS lifetime_value
	FROM
		CustomerPurchaseHistory
),
segment_analysis AS (
    SELECT 
        customer_segment,
        customer_status,
        COUNT(customer_id) AS num_customers,
        AVG(tenure_days) AS avg_tenure_days,
        AVG(total_spent) AS avg_lifetime_value,
        AVG(predicted_annual_value) AS avg_predicted_annual_value
    FROM 
        customer_life_time_analysis
    GROUP BY 
        customer_segment, customer_status
),
churn_analysis AS (
    SELECT 
        country,
        state,
        city,
        customer_segment,
        COUNT(customer_id) AS churned_customers,
        AVG(total_spent) AS avg_lifetime_value
    FROM 
        customer_life_time_analysis
    WHERE 
        customer_status = 'Churned'
    GROUP BY 
        country, state, city, customer_segment
)
SELECT 
    * 
FROM 
    customer_life_time_analysis
ORDER BY 
    lifetime_value DESC;

-- Segment Analysis
SELECT 
    * 
FROM 
    segment_analysis
ORDER BY 
    avg_lifetime_value DESC;
    
-- Churn Analysis
SELECT 
    * 
FROM 
    churn_analysis
ORDER BY 
    churned_customers DESC;

-- Q8. If data on promotional campaigns (discounts, events, email marketing) is available, 
-- how could you measure their impact on customer acquisition, retention, and overall sales?
-- -- Answered in Word File

-- Q9. How would you approach this problem, if the objective and subjective questions weren't given?
-- -- Answered in Word File

-- Q10. How can you alter the "Albums" table to add a new column named 
-- "ReleaseYear" of type INTEGER to store the release year of each album?

ALTER TABLE album
ADD COLUMN release_year INT NULL;

-- Q11. Chinook is interested in understanding the purchasing behaviour of customers based on their geographical location. 
-- They want to know the average total amount spent by customers from each country, 
-- along with the number of customers and the average number of tracks purchased per customer. 
-- Write an SQL query to provide this information.

WITH CustomerTotalPurchaseBehaviour AS (
	SELECT
		c.customer_id,
        c.country,
        SUM(i.total) as total_spent,
        COUNT(DISTINCT il.track_id) AS number_of_tracks_purchased
	FROM
		customer c
	JOIN
		invoice i ON i.customer_id = c.customer_id
	JOIN
		invoice_line il ON il.invoice_id = i.invoice_id
	GROUP BY
		c.customer_id, c.country
)
SELECT
	country,
    COUNT(DISTINCT customer_id) AS number_of_customers,
    ROUND(AVG(total_spent),2) AS average_total_amount_spent_by_customer,
    ROUND(AVG(number_of_tracks_purchased),2) AS average_number_of_tracks_purchased
FROM 
	CustomerTotalPurchaseBehaviour
GROUP BY
	country
ORDER BY
	number_of_customers DESC;


-- extra

SELECT COUNT(distinct country) FROM customer;

SELECT COUNT(distinct genre_id) FROM genre;

SELECT country, COUNT(distinct customer_id) FROM customer
GROUP BY country




        