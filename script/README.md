Дополнительные эндпойнты
<hr>
Детальное описание:
```
https://op.itmo.ru/api/workprogram/detail/ + str(<'НАШ_ИД' дисциплины>)
```

Из детального описания дисциплины взять только структурное подразделение (structural_unit).

Пример:
```
"structural_unit": {
        "id": 7,
        "title": "высшая школа цифровой культуры",
        "isu_id": 648
    }
```

Для дэшборда, можно делать визуалазицию ещё по одному измерению "структурные подразделения".

<hr>
Статус экспертизы:
```
https://op.itmo.ru/api/workprogram/isu/ + str(<'DISC_DISC_ID' дисциплины>)
```

Пример ответа:
```
{
    "title": "Алгебры Ли и группы Ли",
    "expertise_status": "В работе",
    "wp_url": "https://op.itmo.ru/work-program/14114"
}
```

Последние цифры в строке wp_url - это <'НАШ_ИД' дисциплины>
<hr>
Соответствие <'НАШ_ИД' дисциплины> и <'DISC_DISC_ID' дисциплины> можно также взять из эндпойнта 
```https://op.itmo.ru/api/record/academic_plan/academic_wp_description/all```