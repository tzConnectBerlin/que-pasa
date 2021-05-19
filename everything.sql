CREATE VIEW all_storage AS (

SELECT l._level as level, s.id as storage_id

SELECT l._level, s.id FROM
       levels l
              INNER JOIN (SELECT id, MAX(_level) AS max_level FROM storage GROUP BY id, _level) s
              ON s.max_level <= l._level
              HAVING COUNT(s.id) = 1
              ORDER BY l._level




select DISTINCT ON (l._level) l._level, s.id FROM
       levels l
              JOIN (SELECT id, MAX(_level) AS max_level FROM storage GROUP BY id, _level) s
              ON s.max_level <= l._level
              ORDER BY l._level



SELECT DISTINCT ON (l._level) l._level, storage.id as storage_id, "storage.market_map".id as market_map_id, "storage.supply_map".id as supply_id, "storage.liquidity_provider_map".id as liquidity_provider_id FROM
       levels l
       LEFT JOIN
       (SELECT id, MAX(_level) AS max_level FROM storage GROUP BY id, _level ORDER BY max_level DESC) as storage ON l._level >= storage.max_level
       LEFT JOIN
       (SELECT id, MAX(_level) AS max_level FROM "storage.market_map" GROUP BY id, _level ORDER BY max_level DESC) AS "storage.market_map"
               ON l._level >= "storage.market_map".max_level
       LEFT JOIN
       (SELECT id, MAX(_level) AS max_level FROM "storage.supply_map" GROUP BY id, _level ORDER BY max_level DESC) AS "storage.supply_map"
               ON l._level >= "storage.supply_map".max_level
       LEFT JOIN
       (SELECT id, MAX(_level) AS max_level FROM "storage.liquidity_provider_map" GROUP BY id, _level ORDER BY max_level DESC) AS "storage.liquidity_provider_map"
               ON l._level >= "storage.liquidity_provider_map".max_level
       ORDER BY _level DESC;


CREATE VIEW storage_all AS SELECT DISTINCT ON (l._level) l._level, storage.id as storage_id, "storage.market_map".id as "storage.market_map_id", "storage.supply_map".id as "storage.supply_map_id", "storage.liquidity_provider_map".id as "storage.liquidity_provider_map_id" FROM
       levels l
       LEFT JOIN
       (SELECT id, MAX(_level) AS max_level FROM storage GROUP BY id, _level ORDER BY max_level DESC) as storage ON l._level >= storage.max_level
       LEFT JOIN
       (SELECT id, MAX(_level) AS max_level FROM "storage.market_map" GROUP BY id, _level ORDER BY max_level DESC) AS "storage.market_map"
               ON l._level >= "storage.market_map".max_level
       LEFT JOIN
       (SELECT id, MAX(_level) AS max_level FROM "storage.supply_map" GROUP BY id, _level ORDER BY max_level DESC) AS "storage.supply_map"
               ON l._level >= "storage.supply_map".max_level
       LEFT JOIN
       (SELECT id, MAX(_level) AS max_level FROM "storage.liquidity_provider_map" GROUP BY id, _level ORDER BY max_level DESC) AS "storage.liquidity_provider_map"
               ON l._level >= "storage.liquidity_provider_map".max_level
       ORDER BY _level DESC;
