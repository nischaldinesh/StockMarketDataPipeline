// Top 5 Lossess

WITH opening_prices AS (  SELECT ticker, price AS opening_price,         
 ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY updated ASC) AS rn   
 FROM stock_data   WHERE price_type = 'opening'     
 AND year = 2025 AND month = 2 AND week = 9 ), closing_prices AS (  SELECT ticker, price AS closing_price,          
 ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY updated DESC) AS rn   
 FROM stock_data   WHERE price_type = 'closing'     
 AND year = 2025 AND month = 2 AND week = 9 ) 
 SELECT o.ticker, o.opening_price, c.closing_price,        
 round(((c.closing_price - o.opening_price) / o.opening_price) * 100, 2) 
 AS weekly_loss FROM opening_prices o JOIN closing_prices c ON o.ticker = c.ticker WHERE o.rn = 1 AND c.rn = 1   
 AND ((c.closing_price - o.opening_price) / o.opening_price) < 0 ORDER BY weekly_loss ASC LIMIT 5


 // Top 5 Gainers

 WITH opening_prices AS (  SELECT ticker, price AS opening_price,          
 ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY updated ASC) AS rn   
 FROM stock_data   WHERE price_type = 'opening'     
 AND year = 2025 AND month = 2 AND week = 7 ), closing_prices AS (  SELECT ticker, price AS closing_price,          
 ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY updated DESC) AS rn   
 FROM stock_data   WHERE price_type = 'closing'     
 AND year = 2025 AND month = 2 AND week = 7 ) SELECT o.ticker, o.opening_price, c.closing_price,        
 round(((c.closing_price - o.opening_price) / o.opening_price) * 100, 2) 
 AS weekly_gain FROM opening_prices o JOIN closing_prices c ON o.ticker = c.ticker WHERE o.rn = 1 AND c.rn = 1   
 AND ((c.closing_price - o.opening_price) / o.opening_price) > 0 ORDER BY weekly_gain DESC LIMIT 5