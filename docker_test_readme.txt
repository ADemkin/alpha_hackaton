Что делать:

1. инсталлировать docker (www.docker.com)
2. инсталлировать модули python
    pip install numpy scipy pandas scikit-learn
    pip install docker

3. запускаем сервер обработки (можно в отдельной консоле, т.к. он будет активным все время):
    python check_solution_server.py

4. запускаем клиент, чтобы понять как это работает:
    python solution_example/run_client.py

    По окончанию выведет строчку:
    Completed! items_processed: 99881, time_elapsed: 7.915 sec, score: 0.217

5. А теперь кульминация! запускаем клиент в контейнере docker:

    5.1 смотрим ip-адрес для docker network bridge:
        в зависимости от системы, набираем в консоле:
            ip addr/ifconfig/ipconfig
        Видим что-то вроде:
            ...bla-bla-bla...
            28: docker_gwbridge: <NO-CARRIER,BROADCAST,MULTICAST,UP> mtu 1500 qdisc noqueue state DOWN group default
            inet 172.18.0.1/16 {<--- ЭТО ОН!!!} scope global docker_gwbridge
            valid_lft forever preferred_lft forever
            ...bla-bla-bla...

    5.2 открываем run_solution_in_docker.py в блокноте и присваиваем этот IP переменной DOCKER_BRIDGE_IP вместо "172.18.0.1" (11-ая строка).

    5.3 нужен доступ в интернет, чтобы docker мог загрузить требуемый image (~600MB, но образ грузится 1 раз).

    5.4 наконец, можно стартовать:

        python run_solution_in_docker.py

        Если по окончанию выведет строчку:
        "Completed! items_processed: 99881, time_elapsed: 9.408 sec, score: 0.217"
        Значит, все ОК
