package pipeline

const (
	RepeatingSavingPipeFail             = "Повторная ошибка при сохранении объекта пайплайна. БД недоступна"
	DBUnavailableNonRetryableSavingPipe = "Неудачная попытка сохранить пайплайн. БД недоступна"
	SavingPipeFail                      = "Неудачная попытка сохранить пайплайн"

	SavingTaskFail                      = "Неудачная попытка сохранить PipelineTask"
	RepeatingSavingTaskFail             = "Повторная ошибка при сохранении объекта PipelineTask. БД недоступна"
	DBUnavailableNonRetryableSavingTask = "Неудачная попытка сохранить PipelineTask. БД недоступна"
)
