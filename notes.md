Base on own observation, this scrapper uses the explorer endpoint which is fine but doesn't export vintages.

This works on vivino: https://www.vivino.com/api/vintages/147380568

https://www.vivino.com/webapi/explore/explore?country_codes%5B%5D=au&currency_code=AUD&min_rating=3.8&page=1&language=en&grape_ids%5B%5D=1

## TODO

extract vintages and build a database model for it

- vintage
  - id
  - year
  - name
  - statistics
  - image
  - wine
    - id
    - name
    ...more
    - winery
      - id
      - name
    - taste:
      ...
- price
  - id
  - amount
    
