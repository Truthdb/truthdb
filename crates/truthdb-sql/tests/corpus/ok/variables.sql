DECLARE @a INT, @b NVARCHAR(10) = 'hi', @c INT = @a + 1;
SET @a = 5 * 2;
SELECT @a AS doubled, id FROM t WHERE v >= @a;
