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
-- ����� 1: ����� ���
(1, N'����', N'���', 'M', NULL, NULL, 6),   
(2, N'���', N'���', 'F', NULL, NULL, NULL),
(3, N'���', N'���', 'M', 1, 2, NULL),
(4, N'����', N'���', 'F', 1, 2, NULL),

-- ����� 2: ����� ���
(5, N'�����', N'���', 'M', NULL, NULL, NULL),
(6, N'���', N'���', 'F', NULL, NULL, 1),     
(7, N'����', N'���', 'M', NULL, NULL, 8),
(8, N'���', N'���', 'F', NULL, NULL, 7),

-- ����� 3: ����� ���
(9, N'���', N'���', 'M', NULL, NULL, 12),    
(10, N'���', N'���', 'F', NULL, NULL, NULL),
(11, N'����', N'���', 'M', 9, 12, NULL),

-- ����� 4: ����� ������
(12, N'����', N'������', 'F', NULL, NULL, 9), 
(13, N'����', N'������', 'M', NULL, NULL, 14),
(14, N'�����', N'������', 'F', NULL, NULL, 13);

--����� ���� ����

--����� ���� �� ���� �����
CREATE TABLE FamilyRelations (
    Person_Id INT,
    Relative_Id INT,
    Connection_Type NVARCHAR(20)
);

-- ��� ��
INSERT INTO FamilyRelations (Person_Id, Relative_Id, Connection_Type)
SELECT Person_Id, Father_Id, N'��'
FROM Persons
WHERE Father_Id IS NOT NULL;

-- ��� ��
INSERT INTO FamilyRelations (Person_Id, Relative_Id, Connection_Type)
SELECT Person_Id, Mother_Id, N'��'
FROM Persons
WHERE Mother_Id IS NOT NULL;

-- ��� ����
INSERT INTO FamilyRelations (Person_Id, Relative_Id, Connection_Type)
SELECT Father_Id, Person_Id, N'��'
FROM Persons
WHERE Father_Id IS NOT NULL;

-- ��� ����
INSERT INTO FamilyRelations (Person_Id, Relative_Id, Connection_Type)
SELECT Mother_Id, Person_Id, N'��'
FROM Persons
WHERE Mother_Id IS NOT NULL;

--��� ���
INSERT INTO FamilyRelations (Person_Id, Relative_Id, Connection_Type)
SELECT Person_Id, Spouse_Id, N'�� ���'
FROM Persons
WHERE Spouse_Id IS NOT NULL;

--���� ������
INSERT INTO FamilyRelations (Person_Id, Relative_Id, Connection_Type)
SELECT p1.Person_Id, p2.Person_Id,
       CASE WHEN p2.Gender = 'M' THEN N'��' ELSE N'����' END
FROM Persons p1
JOIN Persons p2
  ON p1.Person_Id <> p2.Person_Id
 AND p1.Father_Id = p2.Father_Id
 AND p1.Mother_Id = p2.Mother_Id
 AND p1.Father_Id IS NOT NULL AND p1.Mother_Id IS NOT NULL;

SELECT * FROM FamilyRelations;

--����� �� ��� ��� ����� ��-����
SELECT 
    p1.Person_Id AS SourceId, 
    p1.Spouse_Id AS TargetId
FROM Persons p1
LEFT JOIN Persons p2 ON p1.Spouse_Id = p2.Person_Id
WHERE 
    p1.Spouse_Id IS NOT NULL
    AND (p2.Spouse_Id IS NULL OR p2.Spouse_Id <> p1.Person_Id);

--����� ��� ��� �����
UPDATE p2
SET Spouse_Id = p1.Person_Id
FROM Persons p1
JOIN Persons p2 ON p1.Spouse_Id = p2.Person_Id
WHERE 
    p2.Spouse_Id IS NULL OR p2.Spouse_Id <> p1.Person_Id;




