CREATE DATABASE FamilyTree;
GO
USE FamilyTree;
GO
CREATE TABLE Persons (
    Person_Id INT PRIMARY KEY,
    Personal_Name NVARCHAR(100),
    Family_Name NVARCHAR(100),
    Gender CHAR(1),
    Father_Id INT NULL,
    Mother_Id INT NULL,
    Spouse_Id INT NULL
);
GO
INSERT INTO Persons (Person_Id, Personal_Name, Family_Name, Gender, Father_Id, Mother_Id, Spouse_Id) VALUES
-- משפחה 1: משפחת לוי
(1, N'אורי', N'לוי', 'M', NULL, NULL, 6),   
(2, N'מיה', N'לוי', 'F', NULL, NULL, NULL),
(3, N'רון', N'לוי', 'M', 1, 2, NULL),
(4, N'נועה', N'לוי', 'F', 1, 2, NULL),

-- משפחה 2: משפחת כהן
(5, N'דניאל', N'כהן', 'M', NULL, NULL, NULL),
(6, N'שרה', N'כהן', 'F', NULL, NULL, 1),     
(7, N'אורי', N'כהן', 'M', NULL, NULL, 8),
(8, N'רות', N'כהן', 'F', NULL, NULL, 7),

-- משפחה 3: משפחת ברק
(9, N'משה', N'ברק', 'M', NULL, NULL, 12),    
(10, N'דנה', N'ברק', 'F', NULL, NULL, NULL),
(11, N'אלון', N'ברק', 'M', 9, 12, NULL),

-- משפחה 4: משפחת ישראלי
(12, N'רבקה', N'ישראלי', 'F', NULL, NULL, 9), 
(13, N'גלעד', N'ישראלי', 'M', NULL, NULL, 14),
(14, N'אילנה', N'ישראלי', 'F', NULL, NULL, 13);

--יצירת טבלה חדשה

--יצירת טבלה של קשרי משפחה
CREATE TABLE FamilyRelations (
    Person_Id INT,
    Relative_Id INT,
    Connection_Type NVARCHAR(20)
);

-- קשר אב
INSERT INTO FamilyRelations (Person_Id, Relative_Id, Connection_Type)
SELECT Person_Id, Father_Id, N'אב'
FROM Persons
WHERE Father_Id IS NOT NULL;

-- קשר אם
INSERT INTO FamilyRelations (Person_Id, Relative_Id, Connection_Type)
SELECT Person_Id, Mother_Id, N'אם'
FROM Persons
WHERE Mother_Id IS NOT NULL;

-- אבא לילד
INSERT INTO FamilyRelations (Person_Id, Relative_Id, Connection_Type)
SELECT Father_Id, Person_Id, N'בן'
FROM Persons
WHERE Father_Id IS NOT NULL;

-- אמא לילד
INSERT INTO FamilyRelations (Person_Id, Relative_Id, Connection_Type)
SELECT Mother_Id, Person_Id, N'בן'
FROM Persons
WHERE Mother_Id IS NOT NULL;

--בני זוג
INSERT INTO FamilyRelations (Person_Id, Relative_Id, Connection_Type)
SELECT Person_Id, Spouse_Id, N'בן זוג'
FROM Persons
WHERE Spouse_Id IS NOT NULL;

--אחים ואחיות
INSERT INTO FamilyRelations (Person_Id, Relative_Id, Connection_Type)
SELECT p1.Person_Id, p2.Person_Id,
       CASE WHEN p2.Gender = 'M' THEN N'אח' ELSE N'אחות' END
FROM Persons p1
JOIN Persons p2
  ON p1.Person_Id <> p2.Person_Id
 AND p1.Father_Id = p2.Father_Id
 AND p1.Mother_Id = p2.Mother_Id
 AND p1.Father_Id IS NOT NULL AND p1.Mother_Id IS NOT NULL;

SELECT * FROM FamilyRelations;

--מציאת כל בני זוג שהקשר חד-צדדי
SELECT 
    p1.Person_Id AS SourceId, 
    p1.Spouse_Id AS TargetId
FROM Persons p1
LEFT JOIN Persons p2 ON p1.Spouse_Id = p2.Person_Id
WHERE 
    p1.Spouse_Id IS NOT NULL
    AND (p2.Spouse_Id IS NULL OR p2.Spouse_Id <> p1.Person_Id);

--השלמת בני זוג בטבלה
UPDATE p2
SET Spouse_Id = p1.Person_Id
FROM Persons p1
JOIN Persons p2 ON p1.Spouse_Id = p2.Person_Id
WHERE 
    p2.Spouse_Id IS NULL OR p2.Spouse_Id <> p1.Person_Id;




