# Why?

MySQL creates highly non efficient dumps from indices perspective.

Here is an example:
```
CREATE TABLE `SOME_TABLE` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `SOME_COLUMN` varchar(255) NOT NULL,
  `OTHER_COLUMN` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UK_SOME_COLUMN` (`SOME_COLUMN`),
  KEY `IDX_OTHER_COLUMN` (`OTHER_COLUMN`)
);

INSERT INTO `SOME_TABLE` (`id`, `SOME_COLUMN`, `OTHER_COLUMN`)
VALUES (...), (...), ... ;
```

If you have millions of rows in a table with indexes it will be restored much longer as if indices were applied after inserts.

So this python scrip just moves index creation statements after inserts:

```
CREATE TABLE `SOME_TABLE` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `SOME_COLUMN` varchar(255) NOT NULL,
  `OTHER_COLUMN` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
);

INSERT INTO `SOME_TABLE` (`id`, `SOME_COLUMN`, `OTHER_COLUMN`)
VALUES (...), (...), ... ;

ALTER TABLE `SOME_TABLE`
ADD UNIQUE KEY `UK_SOME_COLUMN` (`SOME_COLUMN`),
ADD KEY `IDX_OTHER_COLUMN` (`OTHER_COLUMN`);
```

# Usage
```
python optimizer.py input.sql output.sql
```
