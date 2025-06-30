from collections import Counter

def split_file(path, lines_per_chunk=100000):
    chunks = []
    with open(path, 'r', encoding='utf-8') as f:
        lines, i = [], 0
        for line in f:
            lines.append(line)
            if len(lines) == lines_per_chunk:
                name = f"chunk_{i}.txt"
                with open(name, 'w', encoding='utf-8') as out:
                    out.writelines(lines)
                chunks.append(name)
                lines, i = [], i + 1
        if lines:
            name = f"chunk_{i}.txt"
            with open(name, 'w', encoding='utf-8') as out:
                out.writelines(lines)
            chunks.append(name)
    return chunks

def count_file(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        return Counter(line.strip() for line in f if line.strip())

def get_top_errors(file_path, N):
    chunks = split_file(file_path)
    total = Counter()
    for chunk in chunks:
        total.update(count_file(chunk))
    return total.most_common(N)

if __name__ == "__main__":
    top = get_top_errors("Logs.txt", 10)
    for code, count in top:
        print(f"{code}: {count}")

# זמן:
# פיצול קובץ: O(T) – כל שורה נכתבת פעם אחת (T = מספר שורות).
# ספירה בכל קובץ חלקי: O(T).
# עדכון ה־Counter הראשי: O(U) – U הוא מספר הקודים הייחודיים.
# מיון למציאת N הגדולים: O(U log N).
# סה"כ זמן כולל:
# O(T + U log N)

# מקום:
# שורות של chunk אחד בזיכרון בכל רגע → O(K * L)
# (K = שורות בחלק, L = אורך ממוצע לשורה)
# סה"כ Counter של כל הקודים הייחודיים → O(U)