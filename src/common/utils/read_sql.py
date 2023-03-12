import os
from typing import Dict


def readSql(wayFile: str, nameSql: str, args: Dict[str, str]):
    """
    Esta função lê um SQL e retorna como string com os *args aplicados. Pro arg ser aplicado tem que colocar um '#' no início e fim.
    Exemplo, se o nome do campos for competence, então seria #competence# no lugar que ele deve fazer a substituição.
    """
    sql = ""
    try:
        with open(os.path.join(wayFile, nameSql), "rt") as sqlfile:
            for row in sqlfile:
                positionFindHashtag = row.find("#")
                if positionFindHashtag >= 0:
                    for key, value in args.items():
                        row = row.replace(f"#{key}#", value)
                sql += row
    except Exception as e:
        print(e)
        sql = ""

    return sql
