package ru.rt.someproject.somegroup.somesome;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class EmulatorThreadResourceWorking {

    // Блокировки доступа к ресурсам. Одна блокировка - один ресурс. Один ресурс может иметь несколько
    // интерфейсов или точек доступа, однако мы всё равно не разрешим обращаться к нему одновременно
    // более, чем одному потоку.

    // Мы не знаем, что за потоки к нам приходят. Мы лишь знаем их поведение с точки зрения времени
    // и порядка захвата ресурсов, точнее отсутствия чёткого порядка.
    // В ином случае, если мы читаем ресурсы чаще, то можем воспользоваться ReentrantReadWriteLock.
    // Останемся в рамках описания постановки задачи с ReentrantLock.
    private final ConcurrentHashMap<String, ReentrantLock> roLocks = new ConcurrentHashMap<>();

    // Список ресурсов. При желании можно изменить немутабельный список на что-то другое (конфигурацию,
    // передаваемую сторонними источниками или же определяемую нами).
    // Мы могли бы создать только ConcurrentHashMap, однако нам требуется случайно определять ресурсы.
    // Для этого создадим List, что бы быстро обращаться к любому элементу.
    private final List<String> roResourcesList = List.of(
              "node3.network.internal.gateway.rt.ru"
            , "node4.network.internal.gateway.rt.ru"
            , "node5.network.internal.hub.rt.ru"
            , "node11.network.internal.hub.rt.ru"
            , "node32.network.internal.balancer.rt.ru");

    private final List<Integer> allResourceKeys;

    /* Будем хранить в структуре описание поведения потока.
        @locksCount - сколько блокировок одновременно сможет использовать поток. То есть, сколько
        ресурсов сможет опрашивать.
        @workTimeFrom - начальное значение времени в секундах, за которое может отработать поток.
        Этот параметр нам нужен только для тестового приложения. На проде время будет зависеть
        от нагрузки приложения, времени отклика ресурса, времени получения данных, времени обработки данных, иного...
        @workTimeTo - конечное значение времени в секундах, за которое может отработать поток.
        Этот параметр нам нужен только для тестового приложения. На проде время будет зависеть
        от нагрузки приложения, времени отклика ресурса, времени получения данных, времени обработки данных, иного...
        @periodInSeconds - период в секундах, через который запускаются в работу потоки. Для продовой среды
        можно использовать cron или даже сложную логику (например, в период высокой нагрузки урежать запуски потоков
        или учащать, если требуется срочно получить данные, можно сделать это управляемым...)
     */
    private record WorkerConfig(short locksCount, short workTimeFrom, short workTimeTo, short periodInSeconds){};

    /*
    * Определим наши потоки (назовём их воркерами). Здесь всего лишь конфигурации.
    * Но в зависимости от них создадим пулл потоков.
    * Тут требуется уточнить, можно выбрать в качестве варианта - ограниченное количество
    * воркеров на каждом хосте, при этом хостов было бы несколько.
    * В таком случае синхронизация через ReentrantLock в общем-то, не понадобится. Однако,
    * возможно использовать ReentrantLock для дополнительной гарантии, что наши потоки не будут несколько
    * раз блокировать одно и то же через тот же redis или zookeeper.
    * */
    private final List<WorkerConfig> roConfigs = List.of(
            new WorkerConfig((short) 2, (short) 3, (short) 12, (short) 10),
            new WorkerConfig((short) 2, (short) 2, (short) 7, (short) 5)
    );

    /*
    * Получим любое число из указанного диапазона @min и @max.
    * */
    private static short GetGeneratedRandomShort(short min, short max) {
        if (min > max) {
            throw new IllegalArgumentException("В диапазоне начальное число должно быть меньше или равно конечному");
        }
        return (short)ThreadLocalRandom.current().nextInt(min, max + 1);
    }

    // Просто подготовим все наши внутренности.
    protected EmulatorThreadResourceWorking() throws InterruptedException {
        List<Integer> resourceKeys = new ArrayList<>();
        for (int i = 1; i <= roResourcesList.size(); i++) resourceKeys.add(i);
        this.allResourceKeys = Collections.unmodifiableList(resourceKeys);
        FillLocksResources();
    }

    /* В зависимости от содержимого конфигурации блокировок заполним словарик.
       Таким образом, мы сможем по имени ресурса выбирать нужную нам блокировку.
       Хотя это вовсе и не обязательно, но мы создадим блокировки заранее.
       Это хорошо, потому что мы сможем ими управлять, не тратить ресурсы на создание "на лету",
       мы не потеряем блокировку, если возникнет не штатная ситуация (какой-нибудь exception).
       Также может возникнуть ситуация, когда ради блокирования одного и того же ресурса создадутся
       разные блокировки.
       Важно! Если мы всё-таки будем разными приложениями создавать блокировки распределенно (redis, zookeeper),
       то защищаться от мультиблокирования придётся уже не на уровне наших локальных ReentrantLock.
     */
    private void FillLocksResources() throws InterruptedException {
        // Вот тут создаём блокировки. Почему бы не воспользоваться Stream API (linq в C# так же хорош)
        roLocks.putAll(
                roResourcesList.stream()
                        .collect(Collectors.toMap(
                                key -> key,
                                _ -> new ReentrantLock()
                        ))
        );

        // Регистрируем наши потоки и запускаем их.
        WorkersStart();
    }

    /*
    * В этом методе мы якобы выполняем какую-то работу, в зависимости от того, к каком ресурсу подключаемся.
    * Время передаётся сюда только для примера, в реальности время работы будет зависеть от ситуации на проде, в основном
    * будет зависеть от ситуации с ресурсом (доступность, отклик).
    * */
    private void DoSomething(String resourseId, short workTimeFrom, short workTimeTo, UUID guid) throws Exception {
        System.out.format("[%s] [%s] Идёт опрос ресурса [%s]. Имя потока [%s]. \n", guid, LocalDateTime.now(), resourseId, Thread.currentThread().getName());

        // Иногда наши потоки будут падать, может быть, не так часто, как здесь (1 к 8),
        // но на всякий случай предусмотрим и такое поведение.
        short randomForErr = GetGeneratedRandomShort((short)1, (short)8);
        if (randomForErr == 4) throw new Exception("Ошибка русской рулетки!");

        // Также предусмотрем поведение - прерывание потоков. Interrupt будет штатно прерывать наш поток.
        // Однако прерываться будет лишь внутренняя логика, если, конечно, в ней это будет предусмотрено.
        // В нашем случае interrupt только создаёт видимость прерывания.
        // В случае с ScheduledExecutorService мы не теряем поток после interrupt(), и он снова
        // продолжит работу в новой итерации. Так не придётся тратить ресурсы на пересоздание потоков.

        // В этой строке сейчас нет никакого смысла, она здесь для демонстрации, однако команда на прерывание потока может прийти внезапно
        // вне текущей инструкции. Например, у нас предусмотрен ShutdownHook.
        if (randomForErr == 5) Thread.currentThread().interrupt();

        try {
            short iterQty = GetGeneratedRandomShort(workTimeFrom, workTimeTo);

            for (short i = 1; i <= iterQty; i++){
                if (Thread.currentThread().isInterrupted()) {
                    System.out.printf("[%s] [%s] Поток [%s] прерван внешней командой.\n", guid, LocalDateTime.now(), Thread.currentThread().getName());
                    return;
                }
                Thread.sleep(1000);
                System.out.printf("[%s] [%s] Поток [%s] работает [%d] сек. с ресурсом [%s]\n", guid, LocalDateTime.now(), Thread.currentThread().getName(), i, resourseId);
            }
        } catch (Exception e) {
            // Не будем экстренно прерывать работу потока никак, кроме return,
            // в таком случае он, возможно, успеет завершить работу штатно.
            System.out.printf("[%s] [%s] Поток был прерван во время работы с ресурсом. Ошибка [%s]\n", guid, LocalDateTime.now(), e);
            return;
        }

        System.out.format("[%s] [%s] Опрос ресурса [%s] завершён. Имя потока [%s]. \n", guid, LocalDateTime.now(), resourseId, Thread.currentThread().getName());
    }

    /*
    * Этот метод будет имитировать работу наших потоков.
    * В описании постановки задач указано, что потоки захватывают несколько ресурсов.
    * Возможно, это оправдывается тем, что работа с ресурсами ведётся не так долго.
    * И поток не будет сильно зависать, занимаясь работой с ресурсами.
    * Однако, можно порекомендовать всё-таки разделить пуллы потоков на условно медленные и условно быстрые.
    * В этой задаче останемся в рамках постановки, что бы не перегружать код и не противоречить заданию.
    * */

    /*
    * Как у нас в приложении:
    * Поскольку мы не управляем порядком захвата, нам ничего не остаётся, как бесконечно пытаться добраться до ресурсов.
    *
    * Как можно (у нас не так!!!):
    * В ином случае правильным подходом будет являться упорядоченная блокировка по очереди с примением разумных
    * таймаутов, в случае обнаружения заблокированных ресурсов другим потоком. То есть мы по очереди пытаемся
    * заблокировать потоки, при отсутствии доступа к ресурсам можем подождать разумное время. Если не выйдет,
    * действительно можно пропустить итерацию.
    *
    * Не будем описывать логику, которая обнаружит в какой-то момент полное отсутствие ресурсов для работы.
    * Однако в промышленной среде рекомендуется воспользоваться потокобезопасным итератором (например, AtomicInteger),
    * что бы просчитать слишком большое количество неуспешных обращений к ресурсам и, возможно, залогировать это
    * или завершить работу приложения. Retry перезапуска нашего приложения в таком случае можно поручить среде запуска,
    * например, докеру или куберу.
    * */
    private void CaptureResources(short resourceCount, short workTimeFrom, short workTimeTo, UUID guid) throws Exception {
        List<String> capturedResourcesList = new ArrayList<>();
        List<Integer> availableKeys = new ArrayList<>(allResourceKeys);

        // Имитируем случайный выбор ресурса (блокировки).
        // Достаточно сделать это один раз. Важно, если коллекция будет большая, такое поведение
        // не будет оптимальным. Мы же делаем это, что бы соответствовать постановке.
        Collections.shuffle(availableKeys);

        try {
            for (short i = 1; i <= resourceCount; i++)
            {
                // Получаем случайный номер ресурса из перемешанной коллекции.
                int lockId = availableKeys.getFirst();

                // Пробуем заблокировать обращения к конретному ресурсу на уровне нашего приложения.
                // Если всё ок, то добавляем в нашу коллекцию заблокированный ресурс.
                String resourseId = roResourcesList.get(lockId - 1);

                // Проверяем доступность ключей ресурсов для обработки.
                if (availableKeys.isEmpty()) {
                    System.out.printf("[%s] [%s] Нет доступных ресурсов для блокировки.\n", guid, LocalDateTime.now());
                    return;
                }

                // Избавление от реентабельности блокировок
                // Мы не должны захватывать более одного раза один и тот же ресурс
                // в одном потоке, хотя класс реентабельных блокировок как раз позволяет проделать такую операцию.
                // Это возможно, но приведёт к тому, что мы потеряем возможность обработать один или более ресурсов,
                // если в нашей итерации слишком большие ограничения по количеству обрабатываемых ресурсов.
                else availableKeys.removeFirst();

                // Если получилось захватить ресурс, добавим его в нашу коллекцию, что бы после можно было освободить.
                // Позволим отдохнуть процессору от слишком частых переключений, возможно, даже за эти маленькие 100 миллисекунд
                // нужные нам ресурсы освободятся.
                if (roLocks.get(resourseId).tryLock(100, TimeUnit.MILLISECONDS)) {
                    capturedResourcesList.add(resourseId);
                    System.out.printf("[%s] [%s] Произошла блокировка ресурса [%s]. \n", guid, LocalDateTime.now(), resourseId);
                }
                else {
                    // Если не получилось заблокировать ресурс, просто останавливаем работу потока на конкретный запуск.
                    System.out.printf("[%s] [%s] Поток столкнулся с заблокированным ресурсом. Итерация будет завершена досрочно. \n", guid, LocalDateTime.now());
                    return;
                }
            }

            // Выполняем работу с захваченными ресурсами.
            capturedResourcesList.forEach(it -> {
                try {
                    DoSomething(it, workTimeFrom, workTimeTo, guid);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        finally {
            // В любом случае освобождаем ресурсы, проверяя, а в текущем ли потоке они вообще заблокированы.
            capturedResourcesList.forEach(res -> {
                if (roLocks.get(res).isHeldByCurrentThread()) {
                    roLocks.get(res).unlock();
                    System.out.printf("[%s] [%s] Освобождён ресурс [%s]. \n", guid, LocalDateTime.now(), res);
                }
            });
        }
    }

    /*
        В этом методе мы определим потоки и их настройки, а также даже поведение.
        Важно! Поведение, конечно же может определяться и вне.
    * */
    private void WorkersStart() throws InterruptedException {
        // Мы можем не получить ресурсы для обработки. Дальнейшая работа не будет иметь смысла.
        // Возможно, здесь стоит проделать retries с таймаутами. Так и сделаем.
        // Но это вовсе не обязательно, запуск нашего приложения может быть
        // инициирован оркестратором, контейнерной средой, иным триггером.
        // Ограничимся десятью попытками.

        short tryIterCount = 0;

        // В нашем случае не возникнет ситуации, когда отсутствуют ресурсы для обработки,
        // поскольку мы заранее определили их вручную.
        while (roLocks.isEmpty())
        {
            if (tryIterCount > 10){
                throw new InterruptedException("Retry прерывание. Слишком много попыток получить список ресурсов!");
            }
            tryIterCount++;
            System.out.println("Отсутствуют ресурсы для обработки!");
            System.out.printf("Следующая попытка в %s\n", LocalDateTime.now().plusSeconds(60));
            Thread.sleep(6000);
        }

        /*
        * Мы откуда-то волшебным образом узнали размер pool.
        * На самом деле, мы можем сами его ограничить либо хардкорно, либо
        * через конфиг, либо через входной параметр.
        *
        * <<Если суммарное время итерации превзойдёт эти значения по причине длительной задержки, текущую итерацию не прерываем, но следующая итерация должна начинаться сразу.>>
        * Так звучит условие из постановки задачи. Мы можем это "накодить сами", но не будем. Такое поведение обеспечивает уже готовый класс пулла потоков - ScheduledExecutorService.
         * */
        ScheduledExecutorService pool = Executors.newScheduledThreadPool(roConfigs.size());

        /*
        * Очень важный момент.
        * Мы заполняем pool потоками, которые будут запускаться по расписанию.
        * В поведение потока мы передаём также интервально время работы (оно будет
        * ограничено интервалом, но каждый раз рассчитываться случайно).
        * Также мы отрегулируем для каждого потока, а сколько он будет брать ресурсов.
        * Разумеется, речь идёт о попытках брать ресурсы, а не о гарантированной блокировке и работе.
        * */

        /*
        * Регистрируем хук.
        * При завершении JVM или работы приложения в течение 60 секунд у работающих задач будет время завершиться.
        * После pool будет принудительно остановлен.
        * На промышленном контуре это может занять продолжительное время.
        * */
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Завершение работы пула потоков...");

            // Завершаем текущие и отменяем следующие.
            pool.shutdownNow();
            try {
                // Даём 15 секунд на штатное завершение. После 15 секунд завершение принудительное.
                if (!pool.awaitTermination(15, TimeUnit.SECONDS)) {
                    System.out.println("Принудительное завершение всех потоков");
                    pool.shutdownNow();
                }
            } catch (InterruptedException e) {
                // Если что-то случится, завершаем текущий поток и пробуем ещё раз штатно завершить
                // работающие потоки пулла.
                Thread.currentThread().interrupt();
                pool.shutdownNow();
            }
            System.out.println("Пул потоков остановлен");
        }));

        // Не нужная конструкция. Здесь используем её, что бы лог выглядел так, как будто задачи запустились последовательно.
        // На промышленном контуре эта конструкция может помешать.
        CountDownLatch startLatch = new CountDownLatch(1);

        System.out.println("Начало регистрации потоков.");
        System.out.println("___________________________");

        roConfigs.forEach(r -> {
            System.out.printf("Регистрация потока. Количество задач: %d, интервал работы от %d до %d, периодичность запуска - %d сек. \n", r.locksCount, r.workTimeFrom, r.workTimeTo, r.periodInSeconds);

            pool.scheduleAtFixedRate(
                    () -> {
                        UUID guid = UUID.randomUUID();
                        try {

                            startLatch.await();
                            LocalDateTime dtFrom = LocalDateTime.now();

                            // На всякий случай зарегистрируем для каждой итерации выполнения потока GUID.
                            // Так будет проще отслеживать, правильно ли работает наше приложение,
                            // соотнося количество обращений наших потоков к ресурсам на итерацию,
                            // время работы, поведение при обнаружении блокировки.

                            // Захват ресурсов начинается здесь.
                            CaptureResources(r.locksCount, r.workTimeFrom, r.workTimeTo, guid);
                            LocalDateTime dtTo =  LocalDateTime.now();

                            // Посчитаем, планирует ли после итерации поток отдохнуть.
                            // Для работы приложения этого не требуется, но мы за охрану труда.
                            short seconds = (short)Duration.between(dtFrom, dtTo).getSeconds();

                            if (r.periodInSeconds <= seconds){
                                System.out.printf("[%s] [%s] Поток [%s] потратил %d сек. на работу и не успеет отдохнуть! \n", guid, LocalDateTime.now(), Thread.currentThread().getName(), seconds);
                            }
                            else {
                                System.out.printf("[%s] [%s] Поток [%s] потратил %d сек. на работу и планирует отдохнуть! \n", guid, LocalDateTime.now(), Thread.currentThread().getName(), seconds);
                            }
                        } catch (Exception ex) {
                            System.out.printf("[%s] [%s] Поток [%s] был аварийно прерван. Текст ошибки [%s] \n", guid, LocalDateTime.now(), Thread.currentThread().getName(), ex.getMessage());
                        }
                    },
                    0, r.periodInSeconds, TimeUnit.SECONDS
            );
        });

        System.out.println("___________________________");
        System.out.println("Окончание регистрации потоков.");
        System.out.println();

        startLatch.countDown();
    }
}