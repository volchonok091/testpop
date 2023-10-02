<?php

try {
    $c_id = $sub_id_gen = $s_id = 0;
    $datetime = date("Y-m-d H:i:s");


    // отличия между серверов (параметры), остальное должно быть идентично по коду
    define('HOST', $_SERVER['SERVER_NAME']); // 'eu2.adsy.ink'
    define('DEBUG', 0);
    define('START', microtime(true));
    set_time_limit(1);
    ini_set('max_execution_time', 1);

    if (DEBUG) {
        error_reporting(E_ALL);
        error_reporting(-1);
        ini_set('error_reporting', E_ALL);
        ini_set('display_errors', 1);
        ini_set('display_startup_errors', 1);
    }

    // можно определить и через switch

    switch (HOST) {
        case 'adsy.ink':
            define('ID_SERVER', 100);
            break; // не 0 чтобы не тоже самое с ошибкой
        case 'eu.adsy.ink':
            define('ID_SERVER', 1);
            break;
        case 'eu2.adsy.ink':
            define('ID_SERVER', 2);
            break;
        case 'eu3.adsy.ink':
            define('ID_SERVER', 3);
            break;
        case 'eu4.adsy.ink':
            define('ID_SERVER', 4);
            break;
        case 'eu5.adsy.ink':
            define('ID_SERVER', 5);
            break;
        case 'eu6.adsy.ink':
            define('ID_SERVER', 6);
            break;
        case 'eu7.adsy.ink':
            define('ID_SERVER', 7);
            break;
        case 'eu8.adsy.ink':
            define('ID_SERVER', 8);
            break;
        case 'eu9.adsy.ink':
            define('ID_SERVER', 9);
            break;
        case 'eu10.adsy.ink':
            define('ID_SERVER', 10);
            break;
        case 'eu11.adsy.ink':
            define('ID_SERVER', 11);
            break;
        case 'eu12.adsy.ink':
            define('ID_SERVER', 12);
            break;
    }
    define('ID_SERVER', 0);
    //////////////////////////////////
    /// функции
    function debug($str = '')
    {
        if (DEBUG) {
            echo $str;
            var_dump((microtime(true) - START));
        }
    }

    function GetLing($link,$sub_id_type=1): string
    {
        global $array_params,$sub_id_number;
        foreach ($array_params as $key => $array_param) {
            // заменим на числовой если нужно
            if ($sub_id_type==2 && $key=='subid') $array_param=$sub_id_number;
            $link = str_replace('{' . $key . '}', urlencode($array_param), $link);
        }
        //debug('тип sub_id: '.$sub_id_type.' сгенерированная ссылка:'.$link);
        return $link;
    }

    function exit204()
    {
        global $connection, $redis;
        $connection->close();
        $redis->close(); // Закрытие соединения с Redis
        http_response_code(204);
        exit();
    }

    function ch_error_log($str = '', $code = 0)
    {
        global $db, $c_id, $s_id, $datetime;
        if (!$db)
            connect_clickhouse();
        if (DEBUG)
            exit(var_dump(
                $str
            ));
        else
            $db->insert('log',
                [
                    [
                        ' error ' . $str, $c_id, $s_id, $datetime, 0, ID_SERVER, $code
                    ],
                ],
                ['error_text', 'c_id', 's_id', 'created_at', 'sub_id', 'server', 'code']
            );
    }

    function connect_clickhouse()
    {
        global $db;
        $config = [
            'host' => '212.162.152.102',
//        'https' => true,
            'port' => '8123',
            'username' => 'admin',
            'password' => 'uqwq51kWI5HvT0r',
        ];
        debug('подключение к clickhouse');
        $db = new ClickHouseDB\Client($config);
        $db->database('admin_main');
        $db->setTimeout(1); // Установка таймаута соединения в 100 мсекунд
        $db->setConnectTimeOut(1); // 5 seconds
    }

    function prepared_query($mysqli, $sql, $params, $types = "")
    {
        $types = $types ?: str_repeat("s", count($params));
        $stmt = $mysqli->prepare($sql);
        $stmt->bind_param($types, ...$params);
        $stmt->execute();
        return $stmt;
    }

    function getRandomString($n): string
    {
        $characters = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
        $randomString = '';

        for ($i = 0; $i < $n; $i++) {
            $index = rand(0, strlen($characters) - 1);
            $randomString .= $characters[$index];
        }

        return $randomString;
    }

    function isDateTime($datetime)
    {
        $format = 'Y-m-d H:i:s';
        $dateTimeObj = DateTime::createFromFormat($format, $datetime);
        return $dateTimeObj && $dateTimeObj->format($format) === $datetime;
    }

    /////////////////////////////////
    /// подключения
    include '../vendor/autoload.php';
    debug('после autoload.php');


///////////////////////////////////////////////
/// подключение к бд и другое
    debug('до подключения к бд');
    $connection = new mysqli('localhost', 'admin_main', 'oBF7c|PKw7S', 'admin_main');
    $redis = new Redis();
    $redis->connect('127.0.0.1');
    $memcached = new Memcached();
    $memcached->addServer('localhost', 11211);
    $exp_mem = 7 * 24 * 60 * 60; // Время жизни данных в секундах (7 дней)
    $db = false;
    // может устарело
    // Проверка, прошло ли более 1 часа (3600 секунд)

    debug('после подключения к бд');


    // сбор полученных данных и подготовка
    $data = [
        'key' => $_GET['key'] ?? '',
        'c_id' => $c_id = $_GET['c_id'] ?? 0,
        'ua' => isset($_GET['ua']) ? substr($_GET['ua'], 0, 200) : '',
        'ip' => isset($_GET['ip']) ? (filter_var($_GET['ip'], FILTER_VALIDATE_IP, FILTER_FLAG_IPV4) !== false ? $_GET['ip'] : '0.0.0.0') : '0.0.0.0',
        'referrer' => isset($_GET['referrer']) ? substr($_GET['referrer'], 0, 240) : '',
        'subid' => $sub_id = isset($_GET['subid']) ? substr($_GET['subid'], 0, 50) : '',
        'subscriber_id' => $_GET['subscriber_id'] ?? '',
        'subscriber_age' => isset($_GET['subscriber_age']) ? (isDateTime($_GET['subscriber_age']) ? $_GET['subscriber_age'] : $datetime) : $datetime,
        'lang_origin' => isset($_GET['lang']) ? $_GET['lang'] : '',
        'lang' => isset($_GET['lang']) ? substr($_GET['lang'], 0, 2) : '',
        'geo' => isset($_GET['geo']) ? substr($_GET['geo'], 0, 2) : '',
    ];

    // простая защита
    if (empty($data['key'])){
        exit204();
    }


///////////////////////////////////////
/// пользователя получим
    // проверяем пользователя (эффективно) 33% нагрузки memcached
    $user_id = $memcached->get('user_' . $data['key']);

    // если такого пользователя точно нет
    if (!$user_id) {
        ch_error_log('нет такого пользователя по ключу:' . $data['key']);
        exit204();
    }
    debug('после получения пользователя');
    ///////////////////////////////////////////////////
    /// тут без так как лучше просто запросом
    $company = prepared_query($connection, "SELECT * FROM campaigns WHERE u_id = ? AND id = ? AND format = 2 AND status = 1", [$user_id, $c_id])->get_result()->fetch_assoc() ?? null;

    if (!$company) {
        ch_error_log("нет такой компании ($c_id) по пользователю ($user_id)");
        exit204();
    }
    if ($company['limit'] != 0) {
        $qpsc = $redis->get('c' . $company['id']);
        if ($qpsc > $company['limit']) {
            //ch_error_log('компании ('.$c_id.') limit ('.$company['limit'].') текущий: '.$qpsc);
            exit204();
        } else {
            $redis->incr('c' . $company['id']);
        }
    }
    debug('после получения компании');


//////////////////////////////////////////
/// генерируем sub_id
    $sub_id_gen = $memcached->get('sub_id_base64_' . $data['c_id'] . '-' . $data['subid']);
    if (empty($sub_id_gen)) {
        $sub_id_gen = strrev($data['c_id'] . '-' . $data['subid']);
        $memcached->set('sub_id_base64_' . $data['c_id'] . '-' . $data['subid'], $sub_id_gen, $exp_mem);
    }
    debug('после получения sub_ids ');

/////////////////////////////////////////
/// black list по sub_id - only 1

    // получаем source_id блокировки
    $sources_id_exclude = $memcached->get('sub_id_exclude_' . $sub_id_gen);

    debug('после получения blacklist');
    ////////////////////////////////////////////////////////////
/// white list or old

    $source_in = [];

    // по текущему sub_id получим старый sub_id (для старых ребят)
    try {
        $sub_id_sql = prepared_query($connection, "SELECT s_id FROM sub_ids_white_list WHERE sub_id = ?", [$data['subid']], "i")->get_result()->fetch_all(MYSQLI_ASSOC)[0] ?? null;
        if ($sub_id_sql) {
            $source_in[] = $sub_id_sql['s_id'];
        }
    } catch (mysqli_sql_exception $exception) {
        ch_error_log($exception);
        exit204();
    }
    //$source_in[] =41; // временно включу
//////////////////////////////////////////
    // исключения только когда нету обработать
    $source_exclude = explode(',', $memcached->get('source_exclude_' . $data['c_id']));
    if (empty($source_exclude[0]))
        $source_exclude = [$sources_id_exclude];
    else
        $source_exclude[] = $sources_id_exclude;

    debug('после получения исключений');
///////////////////////////////////////////////////////////

    $sql = "SELECT id,qps,subid_type,link,type_pay,result FROM source_pushes WHERE status = ? AND `format` = 2";
    $array_params = [1];
    if ($company['type'] == 1) { // только mainstream
        $sql .= " AND type = ?";
        $array_params[] = 1;
    }

    if (count($source_exclude) >= 1) {
        $sources_id = implode(',', $source_exclude);
        $sql .= " AND id NOT IN (?)";
        $array_params[] = $sources_id;
    }

    if (count($source_in) >= 1) {
        $sources_id = implode(',', $source_in);
        $sql .= " or (status=0 AND id IN (?))";
        $array_params[] = $sources_id;
    }

    $sources = prepared_query($connection, $sql, $array_params)->get_result()->fetch_all(MYSQLI_ASSOC);

    debug('после получения source push');
    /////////////////////////////////
    /// обработка ответо source

    $array_ids = [];
    $array_request = [];
    $need_subid_number=false;
    foreach ($sources as $source) {
        $qps = $redis->get('s' . $source['id']);
        //debug('source: '.$source['id'].' qps: '.$qps);
        if ($qps < $source['qps'] or $source['qps'] == 0) {
            if ($source['subid_type'] == 2)
                $need_subid_number=true;

            $array_request[] = [
                'id' => $source['id'],
                'link' => $source['link'],

                'sub_id_type'=>$source['subid_type'],
                'type_pay' => $source['type_pay'],
                'result' => json_decode(trim(stripslashes($source['result']), '"'), true),
            ];
            if ($source['qps'] != 0) {
                $redis->incr('s' . $source['id']);
            }

        } else {
            //ch_error_log("limit qps ($qps) для source:".$source['id']);
        }
    }

    debug('после qps');

    //////////////////////////////////////////
/// генерируем sub_id number

    $sub_id_number=100000;
    if ($need_subid_number) {
        try {
            $sub_id_sql = prepared_query($connection, "SELECT id FROM sub_ids_number WHERE sub_id = ? ", [$sub_id_gen], "s")->get_result()->fetch_row();
            if (!$sub_id_sql) {
                $generate = random_int(100000, 999999999); // exoclick 6-10 number
                $sub_id_sql = prepared_query($connection, "INSERT INTO sub_ids_number (id, sub_id)  VALUES (?,?)", [$generate, $sub_id_gen], 'is');
                $sub_id_number = $generate;
            } else {
                $sub_id_number = $sub_id_sql[0];
            }
        } catch (mysqli_sql_exception) {
            http_response_code(204);
            exit();
        }
        debug('после получения sub_ids number:' . $sub_id_number);
    }
    ///////////////////////////
    $array_results = [];
    $promises = [];

    $array_params = [
        'ip' => $data['ip'],
        'lang' => $data['lang_origin'],
        'lang_2' => $data['lang'],
        'subid' => $sub_id_gen,
        'domain' => $data['referrer'],
        'user_agent' => $data['ua'],
        'subscriber_id' => $data['subscriber_id'],
        'created_at' => $data['subscriber_age'],
        'created_unix' => strtotime($data['subscriber_age'])
    ];

    debug('перед multicurl');

    $array_res = [];
    $running = 0;
    $key_main = [];

    foreach ($array_request as $key => $item) {
        $key_main[$key]['curl'] = curl_init();
        $key_main[$key]['link'] = GetLing($item['link'],$item['sub_id_type']);
        $key_main[$key]['id'] = $item['id'];
        $key_main[$key]['log'] = $item['log'];
        $key_main[$key]['type_pay'] = $item['type_pay'];
        $key_main[$key]['result'] = $item['result'];
        curl_setopt($key_main[$key]['curl'], CURLOPT_TIMEOUT_MS, 50); //timeout
        curl_setopt($key_main[$key]['curl'], CURLOPT_CONNECTTIMEOUT_MS, 100); //timeout
        curl_setopt($key_main[$key]['curl'], CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($key_main[$key]['curl'], CURLOPT_URL, $key_main[$key]['link']);
    }


    $mh = curl_multi_init();
    foreach ($array_request as $key => $item) {
        curl_multi_add_handle($mh, $key_main[$key]['curl']);
    }

    do {
        $status = curl_multi_exec($mh, $active);
        if ($active) {
            curl_multi_select($mh);
        }
    } while ($active && $status == CURLM_OK);
    $result_answer = [];

    foreach ($key_main as $key => $item) {
        $id = $item['id'];
        $log = $item['log'];
        $result = $item['result'];
        $type_pay = $item['type_pay'];
        $answer = curl_multi_getcontent($item['curl']);
        $code = curl_getinfo($item['curl'], CURLINFO_HTTP_CODE) ?? 420;

        if ($answer == "") {
            $log_answer = null;
        } else {
            $log_answer = $answer;
        }

        if ($answer) {
            $result_answer[$key]['curl'] = $answer;
            $result_answer[$key]['id'] = $id;
            $result_answer[$key]['log'] = $log;
            $result_answer[$key]['result'] = $result;
            $result_answer[$key]['code'] = $code;
            $result_answer[$key]['type_pay'] = $type_pay;
        }
        curl_multi_remove_handle($mh, $item['curl']);
    }

    curl_multi_close($mh);

    debug('после multicurl');
    ///////////////////////////////////////////
    /// Проверка на запрещенные домены в мейнстрим

    // можно бд или меморикеш
    $domain_main_exclude = $memcached->get('domain_main_exclude');
    if (!$domain_main_exclude) {
        $forbiddenDomains = $connection->query("SELECT `domain` FROM domain_main_exclude")->fetch_all(MYSQLI_ASSOC);
        // сериализуйте их перед сохранением в Memcached
        $serializedData = serialize($forbiddenDomains);
        $memcached->set('domain_main_exclude', $serializedData, $exp_mem);
    } else {
        // Если данные найдены в Memcached, десериализуйте их перед использованием
        $forbiddenDomains= unserialize($domain_main_exclude);
    }


    function isDomainAllowed($url='',$forbiddenDomains=[]): bool
    {
        global $connection;
        $parsedUrl = parse_url($url);
        $domain = $parsedUrl['host'];

        foreach ($forbiddenDomains as $forbiddenDomain) {
            if (str_contains($domain, $forbiddenDomain['domain'])) {
                debug('ДОМЕН запрещен в mainstream ('.$domain.')');
                return false; // Домен найден в списке запрещенных
            }
        }
        return true; // Домен не найден в списке запрещенных
    }

    //////////////////////////////////////////////
    /// обработка ответов
    if (isset($result_answer)) {
        foreach ($result_answer as $key => $response) {
            try {
                if (isset($response['curl']) and !empty($response['curl'])) {
                    $response_curl = json_decode($response['curl'], true);

                    $result = $response_curl['seatbid'][0]['bid'][0] ?? $response_curl['result']['listing'][0] ?? $response_curl['ads'][0] ?? $response_curl[0] ?? $response_curl; //?? $response_curl['bid']

                    //exoclik
                    if (isset($response_curl['bid']['value']) && $response_curl['bid']['value']>0)
                        $result =$response_curl['bid'];

                    // проверяем цену и получения
                    if (isset($response['result']['sum']) && isset($result[$response['result']['sum']])) {
                        if ($response['type_pay'] == 2) {
                            $result[$response['result']['sum']] = $result[$response['result']['sum']] / 1000;
                        }
                        if (isset($array_results[0])) {
                            if ($array_results[0]['sum'] >= $result[$response['result']['sum']]) {
                                continue;
                            }
                        }

                        // проверим урл
                        $allowed_url=$result[$response['result']['click_url']];
                        if ($company['type']==1 && (!isDomainAllowed($allowed_url, $forbiddenDomains)))
                            continue;

                        $array_results[0] = [
                            'sum' => $result[$response['result']['sum']], // sum => bid
                            'click_url' => $allowed_url, // click_url => url
                            'id' => $response['id'],
                        ];
                    }
                } else {
                    continue;
                }
            } catch (Error $error) {
                ch_error_log('Ошибка при декодировании: ' . $error);

            }
        }
    }

    debug('после обработки ответов');
    ///////////////////////////////////////////

    $hash = getRandomString(40);
    $price2 = (isset($array_results[0])) ? $array_results[0]['sum'] * $company['bid'] : null;
    $data_json = (isset($array_results[0])) ? json_encode($array_results[0]) : null;
    $partner = $array_results[0]['id'] ?? null;

    if (isset($array_results[0])) {
        $array_results[0]['click_url'] = str_replace('{sub_id}', $sub_id_gen, $array_results[0]['click_url']);

        $price = round($array_results[0]['sum'] * $company['bid'], 8);

        if ($price == 0) {
            ch_error_log('Получена пустая цена');
            exit204();
        }

        echo json_encode([
            'price' => ($company['type_pay'] == 1) ? $price : $price * 1000,
            'click_url' => "http://" . HOST . "/clickPop/?hash=$hash"
        ]);

        prepared_query($connection, "INSERT INTO stat_pops_new 
    (hash,c_id, price, partner, data, created_at) VALUES (?,?,?,?,?,?)", [
            $hash, $c_id, $price2, $partner, $data_json, $datetime
        ], "sisiss");

        if (!$db) // если не подключен подключить по необходимости
            connect_clickhouse();
        try {
            $db->insert('stats_pop_merge_day',
                [
                    [
                        $c_id,
                        $data['ua'],
                        $data['ip'],
                        $data['geo'],
                        $data['referrer'],
                        $sub_id,
                        substr($sub_id_gen, 0, 50),
                        $data['subscriber_id'],
                        $data['subscriber_age'],
                        $data['lang_origin'],
                        $price2,
                        $array_results[0]['sum'] ?? null,
                        $partner,
                        (isset($array_results[0])) ? "http://" . HOST . "/clickPop/?hash=$hash" : null,
                        $data_json,
                        $datetime,
                        (microtime(true) - START),
                        ID_SERVER
                    ],
                ],
                ['c_id', 'ua', 'ip', 'country', 'referrer', 'sub_id_old', 'sub_id_new', 'subscriber_id', 'subscriber_age', 'lang', 'price', 'original_price', 'partner', 'link', 'data', 'created_at', 'execute_time', 'server']
            );
        } catch (Exception $exception) {
        } // сбросим ошибки если они были


    } else {
        //ch_error_log('не получены результаты');
        http_response_code(204);
    }

    $connection->close();

    debug('после генерации ответа и цены');
    ///////////////////////////////////////////

    // выведем все сорсы учавствующие и выйгрывшие
    if (DEBUG) {
        var_dump('Все полученные сорсы');
        var_dump($sources);
        var_dump('После фильтрации по qps');
        //var_dump($array_request);

        var_dump('ссылки расчитанные');
        var_dump($key_main);

        $qps = $redis->get('s' . 41); // отслеживать количество
        var_dump('Те кто нам ответил:');
        var_dump($result_answer);
        var_dump('Победитель');
        var_dump($array_results);
    }

    debug('после отправки статистики');
    exit();
} catch (Exception $exception) {
    ch_error_log('Общая ошибка:' . $exception);
    http_response_code(204);
    exit();
}
