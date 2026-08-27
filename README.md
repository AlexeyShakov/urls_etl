# Pipeline Processing Pet Project

## ТЗ
Задача:
Дан список из 1 миллиона url, которые нужно опросить. Из каждого url получить несколько item id и
опросить 3 независимых сервиса c этими данными для данных из всех 3х сервисов собрать итоговый ответ и
сохранить результат в список. После этого список нужно обработать, обработка элементов списка CPU-bound

```
requests_samples = [
  ('http://some-service/getItems/', {'user_id': 100}),  # вернет {item_ids: [1, 2, 3]}
  ('http://some-service/getItems/', {'user_id': 101}),
    ...
]

service_1_url = 'http://service1/fillItems/'
service_2_url = 'http://service2/scoreItems/'
service_3_url = 'http://service3/logItems/'
```

## Описание проекта

Тренировочный pet-project на Go для практики:

- goroutines
- channels
- worker pool
- fan-out / fan-in
- rate limiting
- backpressure
- graceful shutdown
- pipeline architecture

Проект представляет собой сервис для обработки большого количества URL.

## Бизнес-логика

1. Пользователь запускает pipeline.
2. Pipeline получает список URL.
3. Сервис делает запрос к каждому URL.
4. Результат запроса отправляется в 3 внешних сервиса.
5. Результаты от каждого сервиса сохраняются в БД.

---

# MVP

## Упрощения первой версии

В первой версии:

- только один пользователь;
- список URL уже находится в памяти;
- pipeline запускается вручную;
- без Kafka и брокеров сообщений;
- без pause/resume;
- без распределённой системы.

---

## Что нужно реализовать

### 1. Pipeline обработки URL

Сервис запускает обработку списка URL.

---

### 2. Worker pool для URL

Несколько worker-ов:

- читают URL из channel;
- делают HTTP-запрос;
- передают результат дальше в pipeline.

---

### 3. Fan-out на 3 внешних сервиса

После обработки URL результат нужно отправить:

- в Service A;
- в Service B;
- в Service C.

---

### 4. Worker-ы для внешних сервисов

Для каждого сервиса:

- свой channel;
- свой worker pool;
- свои HTTP-запросы.

---

### 5. Rate limiting

Нужно ограничивать скорость запросов к внешним сервисам.

Изучить:

- `net/http.Transport`
- `MaxConnsPerHost`
- token bucket
- leaky bucket
- liquid bucket

---

### 6. Сохранение результатов

Результат ответа каждого сервиса сохраняется в БД.

---

### 7. Backpressure

Использовать bounded channels, чтобы:

- не создавать миллион goroutine;
- ограничивать нагрузку;
- естественно замедлять pipeline при медленной БД или внешних сервисах.

---

### 8. Context и graceful shutdown

Использовать:

- `context.Context`
- корректную остановку worker-ов;
- завершение pipeline без потери данных.

---

<details>
<summary><strong>Общая схема</strong></summary>

![Architecture](docs/arch.png)

### Основные компоненты

#### RunPipeline

`RunPipeline` запускает pipeline и отвечает за создание исходных задач.

Для каждого URL:

1. Создает `Pipeline` и `PipelineTask` в БД.
2. Формирует `PipelineData` со стадией `GetItems`.
3. Регистрирует новую задачу в `PipelineCoordinator`.
4. Отправляет задачу в `requestChannel`.

После отправки всех исходных URL вызывает `FinishInitialUrlsSubmission()` и ожидает завершения всех задач через `Wait()`.

---

#### requestChannel

Общий канал для HTTP-задач всех стадий:

- `GetItems`;
- `FillItems`;
- `ScoreItems`;
- `LogItems`.

В него могут писать несколько producer-ов:

- `RunPipeline` — исходные `GetItems`;
- `HandleStageResult` — новые задачи, появившиеся после обработки `GetItems`.

Из канала задачи забирает `DispatchRequests`.

---

#### DispatchRequests

Распределяет задачи из `requestChannel` между HTTP worker-ами.

```text
requestChannel
      |
      v
DispatchRequests
   /       \
  v         v
workerCh1  workerCh2
```

Таким образом, количество одновременно выполняющихся HTTP-запросов ограничивается количеством `RequestWorker`.

---

#### RequestWorker

`RequestWorker` ничего не знает о бизнес-логике стадий и сохранении результатов в БД.

Worker:

1. Получает `PipelineData`.
2. Выполняет HTTP-запрос.
3. Формирует `RequestResult`.
4. Передает результат в `stageResultCh`.

```text
PipelineData
     |
     v
RequestWorker
     |
 HTTP request
     |
     v
RequestResult
     |
     v
stageResultCh
```

---

#### stageResultCh

Общий канал результатов HTTP-запросов.

Несколько `RequestWorker` являются producer-ами этого канала, а `HandleStageResult` является consumer-ом.

---

#### HandleStageResult

Отвечает за обработку результата завершенной стадии.

Для каждого `RequestResult`:

1. Определяет статус стадии.
2. Сохраняет результат стадии в БД.
3. Только после успешного сохранения решает, требуется ли дальнейшая обработка.

Если HTTP-запрос завершился ошибкой или результат не удалось сохранить в БД, следующие стадии не запускаются.

Для конечных стадий:

```text
FillItems
ScoreItems
LogItems
```

дальнейшая маршрутизация не требуется.

---

### Fan-out после GetItems

`GetItems` возвращает список `item_ids`:

```json
{
  "item_ids": [1, 2, 3]
}
```

После успешного выполнения и сохранения `GetItems` результат используется для создания трех независимых задач:

```text
                   GetItems
                      |
                      v
              HandleStageResult
                /     |     \
               /      |      \
              v       v       v
        FillItems  ScoreItems  LogItems
```

Для каждой стадии используется отдельный builder:

- `BuildFillItemsRequest`;
- `BuildScoreItemsRequest`;
- `BuildLogItemsRequest`.

Builder формирует новый `PipelineData` с нужным URL, payload и `Stage`.

Полученные задачи снова отправляются в общий `requestChannel`, поэтому для выполнения всех HTTP-стадий используется один и тот же worker pool.

---

### PipelineCoordinator

Pipeline имеет динамическое количество задач.

Изначально существует одна задача `GetItems` на каждый URL, но после ее выполнения могут появиться еще три:

```text
GetItems
   |
   +-- FillItems
   +-- ScoreItems
   +-- LogItems
```

Поэтому завершение цикла по исходным URL еще не означает завершение pipeline.

Для определения момента полного завершения используется `PipelineCoordinator`.

Он хранит:

- `counter` — количество зарегистрированных, но еще не завершенных задач;
- `initialUrlsSubmissionFinished` — признак того, что все исходные URL уже отправлены.

Основные операции:

```text
Add(n)                       зарегистрировать новые задачи
Done()                       завершить одну задачу
FinishInitialUrlsSubmission  исходные URL закончились
Wait()                       дождаться полного завершения
```

Pipeline считается завершенным только когда одновременно выполняются условия:

```text
initialUrlsSubmissionFinished == true
counter == 0
```

#### Регистрация задачи

`Add()` должен выполняться **до публикации задачи в `requestChannel`**:

Это гарантирует, что задача будет зарегистрирована до того, как другой goroutine сможет получить ее и вызвать `Done()`.

После отправки всех исходных URL `RunPipeline` выполняет:

```go
pipelineCoordinator.FinishInitialUrlsSubmission()
pipelineCoordinator.Wait()
```

`Wait()` ожидает завершения всего дерева задач, а не только исходных `GetItems`.

---

### Синхронизация goroutine

В приложении используются два разных механизма синхронизации, решающих разные задачи.

`PipelineCoordinator` отслеживает **логическое завершение pipeline**:

```text
Все исходные URL отправлены
        +
Нет незавершенных jobs
        =
Pipeline завершен
```

`sync.WaitGroup` используется для отслеживания **жизненного цикла goroutine**.

При закрытии приложения важно не только понять, что бизнес-задачи закончились(исходные URLs), но и дождаться завершения worker-ов и других goroutine.

Для разных групп goroutine могут использоваться отдельные `WaitGroup`, поскольку закрытие channels выполняется поэтапно и зависит от завершения producer-ов предыдущего уровня.

---

### Закрытие channels

Закрытие выполняется в порядке движения данных.

#### 1. requestChannel

В `requestChannel` пишут:

- `RunPipeline`;
- маршрутизация после `GetItems`.

Поэтому его нельзя закрывать сразу после завершения цикла по исходным URL.

Сначала `PipelineCoordinator.Wait()` должен подтвердить, что все динамически созданные задачи завершены.

После этого новых HTTP-задач уже не появится и `requestChannel` можно закрыть.

```text
PipelineCoordinator.Wait()
        |
        v
close(requestChannel)
```

#### 2. worker channels

После закрытия `requestChannel` `DispatchRequests` дочитывает оставшиеся сообщения.

Когда канал полностью опустел, `range requestChannel` завершается.

Так как `DispatchRequests` является единственным producer-ом `firstWorkerCh` и `secondWorkerCh`, он может закрыть их:

```text
requestChannel closed
        |
        v
DispatchRequests finishes
        |
        +--> close(firstWorkerCh)
        |
        +--> close(secondWorkerCh)
```

#### 3. stageResultCh

После закрытия worker channels `RequestWorker` дочитывают оставшиеся задачи и завершаются.

Несколько `RequestWorker` пишут в один `stageResultCh`, поэтому ни один отдельный worker не должен закрывать этот канал.

Сначала через `WaitGroup` ожидается завершение **всех** `RequestWorker`.

Только после этого coordinator/orchestrator может безопасно выполнить:

```go
close(stageResultCh)
```

#### 4. HandleStageResult

После закрытия `stageResultCh` `HandleStageResult` дочитывает оставшиеся результаты.

Когда channel становится пустым, цикл

```go
for result := range resultCh
```

завершается и `HandleStageResult` прекращает работу.

Итоговая последовательность shutdown:

```text
PipelineCoordinator.Wait()
        |
        v
close(requestChannel)
        |
        v
DispatchRequests finishes
        |
        v
close(worker channels)
        |
        v
RequestWorkers finish
        |
        v
close(stageResultCh)
        |
        v
HandleStageResult finishes
        |
        v
Все goroutine завершены
```

Таким образом, `PipelineCoordinator` отвечает за понимание того, **когда закончилась работа pipeline**, а `WaitGroup` и последовательное закрытие channels — за **корректное завершение goroutine и инфраструктуры pipeline**.

</details>

---

# Что изучить

## Go concurrency

- goroutines
- channels
- select
- sync.WaitGroup
- mutex / RWMutex
- worker pool pattern
- fan-out / fan-in

## HTTP

- `net/http`
- `http.Transport`
- `MaxConnsPerHost`
- `MaxIdleConns`
- `MaxIdleConnsPerHost`
- connection pooling

## Reliability

- graceful shutdown
- backpressure
- rate limiting
- retry policy
- context cancellation

---

# Идеи для дальнейшего улучшения

## 1. Сохранение каждого stage в БД

Хранить не только финальный результат, но и состояние каждого этапа:

- URL получен;
- URL обработан;
- запрос в Service A выполнен;
- запрос в Service B выполнен;
- запрос в Service C выполнен;
- результат сохранён;
- ошибка на конкретном этапе.

---

## 2. Трекинг прогресса pipeline

Отображать:

- сколько задач ожидает;
- сколько в обработке;
- сколько успешно завершено;
- сколько упало;
- сколько задач находится на каждом stage.

---

## 3. Статистика

Собирать:

- общее количество URL;
- количество успешных/неуспешных;
- throughput;
- среднее время обработки;
- ошибки по сервисам;
- retry count.

---

## 4. Pause / Continue

Pipeline можно:

- поставить на паузу;
- продолжить позже.

Идея:

- worker-ы перестают брать новые задачи;
- текущие задачи завершаются;
- состояние сохраняется в БД;
- при continue незавершённые задачи снова попадают в pipeline.

---

## 5. Конфигурация worker-ов

Настраиваемое количество worker-ов:

- URL workers;
- Service A workers;
- Service B workers;
- Service C workers;
- DB workers.

---

## 6. Динамическое масштабирование worker-ов

Возможность:

- добавлять worker-ы во время выполнения pipeline;
- ускорять обработку.

---

## 7. Retry policy

Retry для временных ошибок:

- timeout;
- network errors;
- 5xx ошибки;
- временные ошибки БД.

---

## 8. Resume после падения приложения

После рестарта приложения:

- поднять незавершённые задачи из БД;
- продолжить pipeline с последнего stage.

---

# Главная идея проекта

- goroutines выполняют работу;
- channels передают задачи между этапами;
- worker-ы ограничивают конкурентность;
- rate limiter ограничивает RPS;
- БД хранит состояние pipeline.
