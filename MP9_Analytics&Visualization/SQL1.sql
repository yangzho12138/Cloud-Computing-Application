SELECT airline, origin_airport, destination_airport
FROM "flights-db"."flights-after"
WHERE origin_airport = 'ORD' and scheduled_departure >= 800 and scheduled_departure < 1200 and month = 12 and day = 25