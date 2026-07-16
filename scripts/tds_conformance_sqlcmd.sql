:setvar SQLCMDMAXVARTYPEWIDTH 20
CREATE TABLE sqlcmd_conf (id INT NOT NULL PRIMARY KEY, name NVARCHAR(20));
GO
INSERT INTO sqlcmd_conf VALUES (1, 'one'), (2, 'two'), (3, NULL);
GO
SELECT id, name FROM sqlcmd_conf ORDER BY id;
GO
SELECT COUNT(*) AS n FROM sqlcmd_conf WHERE name IS NOT NULL;
GO
BEGIN TRANSACTION;
UPDATE sqlcmd_conf SET name = 'uno' WHERE id = 1;
ROLLBACK;
GO
SELECT name FROM sqlcmd_conf WHERE id = 1;
GO
