/**
 * Файл: server.cpp
 * 
 * TCP-сервер мессенджера
 * 
 * ============================================================================
 * ОБЩАЯ АРХИТЕКТУРА
 * ============================================================================
 * 
 * - Главный поток: создаёт сокет, слушает порт 8080, принимает новых клиентов
 * - Для каждого клиента создаётся отдельный поток (detached)
 * - Каждый поток независимо обрабатывает своего клиента
 * 
 * ============================================================================
 * МНОГОПОТОЧНОСТЬ
 * ============================================================================
 * 
 * - Сервер: на каждого клиента создаётся отдельный поток
 * - Защита общих данных: мьютекс clients_mutex защищает все глобальные map
 * - Каждый клиентский поток работает независимо, но при доступе к общим
 *   структурам (clients, user_to_socket, active_chats, active_groups)
 *   блокирует мьютекс
 * 
 * ============================================================================
 * ХРАНИЛИЩЕ
 * ============================================================================
 * 
 * - SQLite (файл messenger.db)
 * - Все сообщения сохраняются в БД и не теряются при перезапуске
 * - Поддержка мягкого удаления (флаги deleted_by_from/deleted_by_to)
 * - Отдельная таблица для скрытых сообщений в группах (deleted_group_messages)
 * 
 * ============================================================================
 * ПРОТОКОЛ
 * ============================================================================
 * 
 * - Текстовые сообщения, разделённые символом \n
 * - Каждое сообщение от клиента должно заканчиваться \n
 * - Сервер также отправляет все сообщения с \n в конце
 * 
 * ============================================================================
 * КОМАНДЫ КЛИЕНТА
 * ============================================================================
 * 
 * Личные чаты:
 *   /chat <ник>          - начать диалог с пользователем
 *   /reply <id> <текст>  - ответить на сообщение
 *   /fwd <id> chat/group - переслать сообщение
 *   /delete <id>         - удалить своё сообщение
 *   /clear               - очистить историю (только для себя)
 *   /exit                - выйти из чата в главное меню
 * 
 * Групповые чаты:
 *   /gcreate <имя>       - создать группу
 *   /gjoin <имя>         - войти в группу
 *   /gleave <имя>        - выйти из группы
 *   /gadd <группа> <ник> - добавить участника (только создатель)
 *   /gkick <группа> <ник>- исключить участника (только создатель)
 *   /gmembers <группа>   - показать участников группы
 *   /mygroups            - показать мои группы
 * 
 * Общие команды:
 *   /users               - показать онлайн пользователей
 *   /quit                - выйти из программы
 */

#include <iostream>
#include <string>
#include <cstring>
#include <thread>
#include <map>
#include <mutex>
#include <vector>
#include <algorithm>
#include <sstream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <ctime>
#include "database.h"

using namespace std;

// ============================================================================
// ГЛОБАЛЬНЫЕ ДАННЫЕ
// ============================================================================

/**
 * clients: отображение "дескриптор сокета" -> "имя пользователя"
 * 
 * Нужно для:
 * - Быстрого определения, какой пользователь какому сокету принадлежит
 * - Отправки сообщений всем онлайн пользователям (команда /users)
 * - Очистки данных при отключении клиента
 * 
 * Защищается мьютексом clients_mutex
 */
map<int, string> clients;

/**
 * user_to_socket: отображение "имя пользователя" -> "дескриптор сокета"
 * 
 * Нужно для быстрой отправки сообщения пользователю по его имени.
 * Когда нужно отправить сообщение пользователю "Bob", мы ищем его сокет
 * в этом map и отправляем данные напрямую.
 * 
 * Защищается мьютексом clients_mutex
 */
map<string, int> user_to_socket;

/**
 * active_chats: отображение "кто" -> "с кем"
 * 
 * Хранит информацию о том, в каком личном чате сейчас находится пользователь.
 * Пример: active_chats["Alice"] = "Bob" означает, что Алиса сейчас переписывается с Бобом.
 * 
 * Зачем нужно:
 * - Когда пользователь отправляет сообщение, мы проверяем, в том ли он чате
 * - Если собеседник тоже в этом чате, отправляем сообщение сразу
 * - Если собеседник не в чате, отправляем уведомление
 * 
 * Защищается мьютексом clients_mutex
 */
map<string, string> active_chats;

/**
 * active_groups: отображение "кто" -> "в какой группе"
 * 
 * Хранит информацию о том, в каком групповом чате сейчас находится пользователь.
 * Пример: active_groups["Alice"] = "C++ Developers" означает, что Алиса сейчас в группе.
 * 
 * Зачем нужно:
 * - При отправке сообщения в группу, оно доставляется только тем участникам,
 *   которые сейчас находятся в этом групповом чате (не в меню)
 * - Остальным отправляется уведомление
 * 
 * Защищается мьютексом clients_mutex
 */
map<string, string> active_groups;

/**
 * clients_mutex: мьютекс для защиты всех вышеперечисленных map
 * 
 * Так как сервер многопоточный, несколько потоков могут одновременно
 * читать или изменять эти структуры. Мьютекс предотвращает состояние гонки.
 * 
 * Пример состояния гонки без мьютекса:
 * - Поток A читает clients["Alice"]
 * - Поток B в это же время удаляет clients["Alice"]
 * - Поток A получает повреждённые данные → краш программы
 * 
 * Мьютекс гарантирует, что только один поток может работать с map в каждый момент времени
 */
mutex clients_mutex;

/**
 * db: глобальный объект для работы с базой данных
 * 
 * Все потоки используют один экземпляр.
 * Внутри класса Database есть свой мьютекс (db_mutex), поэтому
 * несколько потоков могут одновременно вызывать методы Database.
 */
Database db;

// ============================================================================
// ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
// ============================================================================

/**
 * Запись сообщения в лог сервера с временной меткой
 * 
 * @param msg - текст сообщения для записи
 * 
 * Лог пишется в стандартный вывод (cout).
 * Формат: "Thu Nov 16 14:30:25 2023 Сообщение"
 * 
 * Используется для отслеживания событий:
 * - запуск/остановка сервера
 * - подключение/отключение пользователей
 * - создание новых пользователей
 */
void log(const string& msg) {
    time_t now = time(0);                     // Текущее время в секундах с эпохи Unix
    cout << ctime(&now) << " " << msg << endl; // ctime() форматирует время
}

/**
 * Отправка сообщения клиенту
 * 
 * @param sock - дескриптор сокета клиента
 * @param msg  - текст сообщения (без символа новой строки)
 * 
 * Протокол общения: каждое сообщение должно заканчиваться символом \n.
 * Клиент ждёт этот символ, чтобы понять, что сообщение получено полностью.
 * 
 * Почему так: TCP — это потоковый протокол, он не сохраняет границы сообщений.
 * Добавляя \n в конце, мы можем разделять сообщения на стороне клиента.
 */
void send_to_client(int sock, const string& msg) {
    string with_newline = msg + "\n";
    send(sock, with_newline.c_str(), with_newline.length(), 0);
}

/**
 * Получение текущей даты по Новосибирску (часовой пояс UTC+7)
 * 
 * @return строка в формате "ДД.ММ.ГГГГ", например "16.11.2023"
 * 
 * Зачем это нужно:
 * - Сервер может находиться в любом часовом поясе
 * - Пользователи ожидают новосибирское время (задание курсовой)
 * - Чтобы получить правильную дату, берём UTC и прибавляем 7 часов
 * 
 * Используется для группировки сообщений по дням при отображении истории
 */
string get_novosibirsk_date() {
    time_t now = time(0);                     // Текущее время в секундах с 1970 года (UTC)
    struct tm* gmt = gmtime(&now);            // Преобразуем в структуру UTC (без часового пояса)
    time_t nsk_time = mktime(gmt) + (7 * 3600); // Прибавляем 7 часов к UTC → получаем Новосибирск
    struct tm* tm_info = localtime(&nsk_time);  // Преобразуем обратно в структуру tm
    char buf[16];
    strftime(buf, sizeof(buf), "%d.%m.%Y", tm_info); // Форматируем дату
    return string(buf);
}

/**
 * Получение текущего времени по Новосибирску (UTC+7)
 * 
 * @return строка в формате "ЧЧ:ММ", например "14:30"
 * 
 * Аналогично get_novosibirsk_date(), но возвращает время.
 * Используется для отображения времени отправки каждого сообщения.
 */
string get_novosibirsk_time() {
    time_t now = time(0);
    struct tm* gmt = gmtime(&now);
    time_t nsk_time = mktime(gmt) + (7 * 3600);
    struct tm* tm_info = localtime(&nsk_time);
    char buf[16];
    strftime(buf, sizeof(buf), "%H:%M", tm_info);
    return string(buf);
}

// ============================================================================
// ФУНКЦИИ ОТОБРАЖЕНИЯ ИСТОРИИ
// ============================================================================

/**
 * Отправка клиенту истории личного чата
 * 
 * @param client_fd - дескриптор сокета клиента
 * @param username  - имя текущего пользователя
 * @param target    - имя собеседника
 * 
 * Формат вывода:
 * 
 * === Чат с Bob ===
 * Команды: /reply <id> <текст>, /fwd <id> chat/group, /clear, /exit
 * 
 * ------------------ 16.11.2023 ------------------
 * (42) [14:30] [Ответ на (41) Bob: Привет!] Alice: Привет, как дела?
 * (43) [14:32] [Переслано от Charlie] Alice: Смотри, что Charlie написал!
 * 
 * Что показывает:
 * - (ID) - номер сообщения в БД (для команд /reply, /delete, /fwd)
 * - [время] - время отправки по Новосибирску
 * - [Ответ на ...] - если сообщение является ответом, показывается цитата
 * - [Переслано от ...] - если сообщение было переслано, указывается автор
 */
void send_chat_history(int client_fd, const string& username, const string& target) {
    // Отправляем заголовок чата и список доступных команд
    send_to_client(client_fd, "\n=== Чат с " + target + " ===");
    send_to_client(client_fd, "Команды: /reply <id> <текст>, /fwd <id> chat/group, /clear, /exit\n");
    
    // Получаем все сообщения между username и target из базы данных
    auto messages = db.getMessages(username, target);
    
    if (!messages.empty()) {
        string last_date = "";  // Для отслеживания смены даты (чтобы рисовать разделитель)
        
        // Перебираем все сообщения
        for (const auto& m : messages) {
            // Парсим строку формата: id|дата|время|отправитель|текст|reply_id|fwd_from
            size_t p_id = m.find('|');
            size_t p1 = m.find('|', p_id + 1);
            size_t p2 = m.find('|', p1 + 1);
            size_t p3 = m.find('|', p2 + 1);
            size_t p4 = m.find('|', p3 + 1);
            size_t p5 = m.find('|', p4 + 1);
            
            // Если формат нарушен (старая версия БД), пропускаем сообщение
            if (p_id == string::npos || p1 == string::npos || p2 == string::npos || p3 == string::npos) 
                continue;
            
            // Извлекаем все части строки
            string msg_id   = m.substr(0, p_id);                      // ID сообщения
            string msg_date = m.substr(p_id + 1, p1 - p_id - 1);      // Дата
            string msg_time = m.substr(p1 + 1, p2 - p1 - 1);          // Время
            string msg_from = m.substr(p2 + 1, p3 - p2 - 1);          // Отправитель
            string msg_text = m.substr(p3 + 1, p4 - p3 - 1);          // Текст
            string reply_id = (p4 != string::npos) ? m.substr(p4 + 1, p5 - p4 - 1) : "0";
            string fwd_from = (p5 != string::npos) ? m.substr(p5 + 1) : "";
            
            // Если сменилась дата, рисуем разделитель
            if (msg_date != last_date) {
                send_to_client(client_fd, "------------------ " + msg_date + " ------------------");
                last_date = msg_date;
            }
            
            // Формируем мета-информацию (ответ или пересылка)
            string meta_info = "";
            
            // Если это ответ на другое сообщение
            if (reply_id != "0" && reply_id != "") {
                string orig = db.getMessageText(stoi(reply_id), false, username);
                if (!orig.empty()) {
                    meta_info += "[Ответ на (" + reply_id + ") " + orig + "] ";
                }
            }
            
            // Если это пересланное сообщение
            if (!fwd_from.empty()) {
                meta_info += "[Переслано от " + fwd_from + "] ";
            }
            
            // Отправляем готовую строку клиенту
            send_to_client(client_fd, "(" + msg_id + ") [" + msg_time + "] " + meta_info + msg_from + ": " + msg_text);
        }
    } else {
        // Нет сообщений в этом чате
        send_to_client(client_fd, "История пуста. Начните переписку!");
    }
    
    send_to_client(client_fd, "");  // Пустая строка для разделения
}

/**
 * Отправка клиенту истории группового чата
 * 
 * @param client_fd  - дескриптор сокета клиента
 * @param username   - имя текущего пользователя
 * @param group_name - название группы
 * 
 * ВАЖНОЕ ОТЛИЧИЕ ОТ ЛИЧНОГО ЧАТА:
 * 
 * 1. Сообщения в группах не удаляются физически (нет флагов deleted_by_*)
 * 2. Пользователи могут только "скрыть" сообщение для себя
 * 3. Информация о скрытых сообщениях хранится в таблице deleted_group_messages
 * 4. При каждом входе в группу скрытые сообщения загружаются из БД,
 *    что гарантирует сохранность этой информации между сессиями
 */
void send_group_history(int client_fd, const string& username, const string& group_name) {
    send_to_client(client_fd, "\n=== Групповой чат: " + group_name + " ===");
    send_to_client(client_fd, "Команды: /reply <id> <текст>, /fwd <id> chat/group, /delete <id>, /clear, /exit\n");
    
    auto messages = db.getGroupMessages(group_name);
    
    // ПОЛУЧАЕМ СПИСОК СКРЫТЫХ СООБЩЕНИЙ ИЗ БД (а не из оперативной памяти)
    // Это ключевое изменение для сохранения состояния между сессиями
    vector<int> my_hidden = db.getHiddenGroupMessages(username);
    
    if (!messages.empty()) {
        string last_date = "";
        for (const auto& m : messages) {
            size_t p_id = m.find('|');
            size_t p1 = m.find('|', p_id + 1);
            size_t p2 = m.find('|', p1 + 1);
            size_t p3 = m.find('|', p2 + 1);
            size_t p4 = m.find('|', p3 + 1);
            size_t p5 = m.find('|', p4 + 1);
            
            if (p_id == string::npos || p1 == string::npos || p2 == string::npos || p3 == string::npos) 
                continue;
            
            int msg_id = stoi(m.substr(0, p_id));
            
            // ПРОПУСКАЕМ СКРЫТЫЕ СООБЩЕНИЯ
            // Проверяем, не скрыл ли пользователь это сообщение
            if (find(my_hidden.begin(), my_hidden.end(), msg_id) != my_hidden.end()) {
                continue;
            }
            
            string msg_date = m.substr(p_id + 1, p1 - p_id - 1);
            string msg_time = m.substr(p1 + 1, p2 - p1 - 1);
            string msg_from = m.substr(p2 + 1, p3 - p2 - 1);
            string msg_text = m.substr(p3 + 1, p4 - p3 - 1);
            string reply_id = (p4 != string::npos) ? m.substr(p4 + 1, p5 - p4 - 1) : "0";
            string fwd_from = (p5 != string::npos) ? m.substr(p5 + 1) : "";
            
            if (msg_date != last_date) {
                send_to_client(client_fd, "------------------ " + msg_date + " ------------------");
                last_date = msg_date;
            }
            
            string meta_info = "";
            if (reply_id != "0" && reply_id != "") {
                string orig = db.getMessageText(stoi(reply_id), true, username);
                if (!orig.empty()) {
                    meta_info += "[Ответ на (" + reply_id + ") " + orig + "] ";
                }
            }
            if (!fwd_from.empty()) {
                meta_info += "[Переслано от " + fwd_from + "] ";
            }
            
            send_to_client(client_fd, "(" + to_string(msg_id) + ") [" + msg_time + "] " + meta_info + msg_from + ": " + msg_text);
        }
    } else {
        send_to_client(client_fd, "История пуста. Начните переписку!");
    }
    send_to_client(client_fd, "");
}

// ============================================================================
// ОСНОВНОЙ ОБРАБОТЧИК КЛИЕНТА (запускается в отдельном потоке для каждого)
// ============================================================================

/**
 * Обработка всех команд и сообщений от одного клиента
 * 
 * @param client_fd - дескриптор сокета для общения с этим клиентом
 * 
 * Эта функция выполняется в отдельном потоке для каждого подключившегося клиента.
 * 
 * ============================================================================
 * ЖИЗНЕННЫЙ ЦИКЛ ПОТОКА
 * ============================================================================
 * 
 * 1. КЛИЕНТ ПОДКЛЮЧАЕТСЯ, ОТПРАВЛЯЕТ НИКНЕЙМ
 *    - Сервер проверяет/создаёт пользователя в БД
 *    - Регистрирует клиента в глобальных map
 * 
 * 2. ГЛАВНОЕ МЕНЮ
 *    - Пользователь может вводить команды: /chat, /gjoin, /gcreate, /mygroups, /users
 *    - При выборе чата или группы переходим в режим диалога
 * 
 * 3. ЛИЧНЫЙ ЧАТ
 *    - Обычный текст → отправляется собеседнику
 *    - /reply <id> <текст> → ответ на сообщение
 *    - /fwd <id> chat/group <куда> → пересылка
 *    - /delete <id> → удалить своё сообщение (мягкое удаление)
 *    - /clear → очистить историю (только для себя)
 *    - /exit → вернуться в главное меню
 * 
 * 4. ГРУППОВОЙ ЧАТ
 *    - Обычный текст → отправляется всем участникам, находящимся в чате
 *    - /reply <id> <текст> → ответ (видят все)
 *    - /fwd <id> chat/group <куда> → пересылка
 *    - /delete <id> → скрыть сообщение только для себя (сохраняется в БД)
 *    - /clear → скрыть все сообщения в группе для себя
 *    - /gleave → выйти из группы
 *    - /exit → вернуться в главное меню
 */
void handle_client(int client_fd) {
    char buffer[4096];          // Буфер для приёма данных
    string username;            // Имя пользователя (после логина)
    bool logged_in = false;     // Прошёл ли пользователь авторизацию
    string current_chat_with;   // С кем сейчас идёт диалог (если в личном чате)
    string current_group;       // В какой группе сейчас (если в групповом чате)
    string incomplete_msg;      // Буфер для накопления неполных сообщений
    
    // Приветствие и запрос ника
    send_to_client(client_fd, "\n========================================");
    send_to_client(client_fd, "         ДОБРО ПОЖАЛОВАТЬ В ЧАТ");
    send_to_client(client_fd, "========================================");
    send_to_client(client_fd, "Введите ваш никнейм: ");
    
    // ========================================================================
    // ГЛАВНЫЙ ЦИКЛ ОБРАБОТКИ СООБЩЕНИЙ ОТ КЛИЕНТА
    // ========================================================================
    while (true) {
        // Принимаем данные от клиента
        memset(buffer, 0, sizeof(buffer));
        int bytes = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
        
        // Если клиент отключился или ошибка
        if (bytes <= 0) {
            if (logged_in) {
                // Логируем выход пользователя
                log("Пользователь " + username + " вышел");
                
                // Очищаем все глобальные структуры от информации об этом пользователе
                lock_guard<mutex> lock(clients_mutex);
                user_to_socket.erase(username);      // Удаляем из обратного отображения
                clients.erase(client_fd);            // Удаляем из прямого отображения
                active_chats.erase(username);        // Очищаем статус чата
                active_groups.erase(username);       // Очищаем статус группы
                // Примечание: hidden_group_msg больше не нужен, данные хранятся в БД
            }
            close(client_fd);  // Закрываем сокет
            break;             // Выходим из цикла, поток завершается
        }
        
        // Накопление данных: TCP может принести сообщение не целиком,
        // поэтому добавляем новые данные в буфер и разбираем по \n
        incomplete_msg += string(buffer);
        
        // Разбираем буфер на отдельные сообщения (разделитель - \n)
        size_t pos;
        while ((pos = incomplete_msg.find('\n')) != string::npos) {
            // Извлекаем одно сообщение
            string msg = incomplete_msg.substr(0, pos);
            incomplete_msg = incomplete_msg.substr(pos + 1);
            
            // =================================================================
            // ЭТАП 1: ПОЛЬЗОВАТЕЛЬ ЕЩЁ НЕ АВТОРИЗОВАН
            // =================================================================
            if (!logged_in) {
                // Первое сообщение от клиента должно содержать никнейм
                username = msg;
                
                // Если пользователь новый, создаём его в БД
                if (!db.userExists(username)) {
                    db.createUser(username);
                    log("Новый пользователь: " + username);
                }
                logged_in = true;
                
                // Регистрируем пользователя в глобальных структурах
                {
                    lock_guard<mutex> lock(clients_mutex);
                    clients[client_fd] = username;                  // fd -> username
                    user_to_socket[username] = client_fd;          // username -> fd
                }
                
                log("Авторизован: " + username);
                
                // Отправляем справку по командам
                string welcome = "\n=== Вы вошли как @" + username + " ===\n";
                welcome += "[ЧАТЫ]:   /users (онлайн) | /chat <ник> (открыть диалог)\n";
                welcome += "[ГРУППЫ]: /mygroups (список) | /gcreate <имя> (создать) | /gjoin <имя> (войти)\n";
                welcome += "          /gadd (добавить) | /gkick (исключить) | /gmembers (участники)\n";
                welcome += "[ОПЦИИ]:  /reply <id> <текст> (ответ) | /fwd <id> chat/group <куда> (пересылка)\n";
                welcome += "          /delete <id> (удалить) | /clear (очистить) | /exit (в меню) | /quit (выход)\n";
                welcome += "========================================================================\n";
                send_to_client(client_fd, welcome);
            }
            // =================================================================
            // ЭТАП 2: ПОЛЬЗОВАТЕЛЬ В ЛИЧНОМ ЧАТЕ
            // =================================================================
            else if (!current_chat_with.empty()) {
                // Команда выхода из чата
                if (msg == "/exit") {
                    send_to_client(client_fd, "\n[Вы покинули чат с " + current_chat_with + "]\n");
                    send_to_client(client_fd, "Введите /chat <ник> для нового диалога\n");
                    {
                        lock_guard<mutex> lock(clients_mutex);
                        active_chats.erase(username);
                    }
                    current_chat_with.clear();
                }
                // Команда ответа на сообщение
                else if (msg.substr(0, 6) == "/reply") {
                    // Формат: /reply 42 Привет, это ответ!
                    string args = msg.substr(6);
                    size_t start = args.find_first_not_of(" ");
                    if (start == string::npos) {
                        send_to_client(client_fd, "\n[Использование: /reply <id> <текст>]\n");
                        continue;
                    }
                    args = args.substr(start);
                    size_t space = args.find(' ');
                    if (space == string::npos) {
                        send_to_client(client_fd, "\n[Ошибка: Введите текст ответа]\n");
                        continue;
                    }
                    
                    // *** ИСПРАВЛЕНИЕ №1: Безопасное преобразование ID в число ***
                    // Раньше при вводе букв вместо цифр сервер падал.
                    // Теперь обрабатываем исключение и продолжаем работу.
                    int reply_id = 0;
                    string reply_text = args.substr(space + 1);
                    try {
                        reply_id = stoi(args.substr(0, space));
                    } catch (...) {
                        send_to_client(client_fd, "\n[Ошибка: ID сообщения должен быть корректным числом!]\n");
                        continue;
                    }
                    
                    string orig = db.getMessageText(reply_id, false, username);
                    if (orig.empty()) {
                        send_to_client(client_fd, "\n[Ошибка: Сообщение с ID " + to_string(reply_id) + " не найдено или уже было удалено]\n");
                        continue;
                    }
                    
                    // Сохраняем ответ с указанием reply_id
                    int new_id = db.saveMessageAdvanced(username, current_chat_with, reply_text, reply_id, "");
                    
                    string short_time = get_novosibirsk_time();
                    string formatted_msg = "(" + to_string(new_id) + ") [" + short_time + "] [Ответ на (" + to_string(reply_id) + ") " + orig + "] " + username + ": " + reply_text;
                    
                    send_to_client(client_fd, formatted_msg);
                    
                    // Отправляем ответ собеседнику, если он онлайн
                    int friend_sock = -1;
                    bool friend_is_chatting_with_me = false;
                    {
                        lock_guard<mutex> lock(clients_mutex);
                        if (user_to_socket.count(current_chat_with)) {
                            friend_sock = user_to_socket[current_chat_with];
                            if (active_chats.count(current_chat_with) && active_chats[current_chat_with] == username) {
                                friend_is_chatting_with_me = true;
                            }
                        }
                    }
                    if (friend_sock != -1) {
                        if (friend_is_chatting_with_me) {
                            send_to_client(friend_sock, formatted_msg);
                        } else {
                            send_to_client(friend_sock, "\n[Уведомление: @" + username + " ответил на сообщение в приватном чате]");
                        }
                    }
                }
                // Команда пересылки сообщения
                else if (msg.substr(0, 4) == "/fwd") {
                    // Формат: /fwd 42 chat Bob  или  /fwd 42 group MyGroup
                    string args = msg.substr(4);
                    stringstream ss(args);
                    int msg_id;
                    string target_type, target_name;
                    if (!(ss >> msg_id >> target_type >> target_name)) {
                        send_to_client(client_fd, "\n[Использование: /fwd <id> chat <ник> или /fwd <id> group <название>]\n");
                        continue;
                    }
                    
                    // Проверяем, что сообщение принадлежит этому чату
                    auto all_my_chat_messages = db.getMessages(username, current_chat_with);
                    bool belongs_to_current_chat = false;
                    for (const auto& m_str : all_my_chat_messages) {
                        size_t first_pipe = m_str.find('|');
                        if (first_pipe != string::npos) {
                            if (m_str.substr(0, first_pipe) == to_string(msg_id)) {
                                belongs_to_current_chat = true;
                                break;
                            }
                        }
                    }
                    
                    if (!belongs_to_current_chat) {
                        send_to_client(client_fd, "\n[Ошибка: Сообщение с ID " + to_string(msg_id) + " не найдено в этом чате]\n");
                        continue;
                    }
                    
                    // Получаем текст сообщения
                    string orig_info = db.getMessageText(msg_id, false, username);
                    if (orig_info.empty()) {
                        send_to_client(client_fd, "\n[Ошибка: Не удалось прочитать текст сообщения или оно удалено]\n");
                        continue;
                    }
                    
                    size_t colon = orig_info.find(": ");
                    string orig_author = orig_info.substr(0, colon);
                    string orig_text = orig_info.substr(colon + 2);
                    
                    // Пересылка в личный чат
                    if (target_type == "chat") {
                        if (!db.userExists(target_name)) {
                            send_to_client(client_fd, "\n[Ошибка: Пользователь " + target_name + " не существует]\n");
                            continue;
                        }
                        
                        int new_fwd_id = db.saveMessageAdvanced(username, target_name, orig_text, 0, orig_author);
                        send_to_client(client_fd, "\n[Сообщение успешно переслано в чат к " + target_name + "]\n");
                        
                        string short_time = get_novosibirsk_time();
                        string formatted_fwd = "(" + to_string(new_fwd_id) + ") [" + short_time + "] [Переслано от " + orig_author + "] " + username + ": " + orig_text;
                        
                        if (target_name == current_chat_with) {
                            send_to_client(client_fd, formatted_fwd);
                        }
                        
                        // Отправляем получателю
                        int target_sock = -1;
                        bool target_is_chatting_with_me = false;
                        {
                            lock_guard<mutex> lock(clients_mutex);
                            if (user_to_socket.count(target_name)) {
                                target_sock = user_to_socket[target_name];
                                if (active_chats.count(target_name) && active_chats[target_name] == username) {
                                    target_is_chatting_with_me = true;
                                }
                            }
                        }
                        if (target_sock != -1) {
                            if (target_is_chatting_with_me) {
                                send_to_client(target_sock, formatted_fwd);
                            } else {
                                send_to_client(target_sock, "\n[Уведомление: @" + username + " переслал сообщение вам в приватный чат]");
                            }
                        }
                    }
                    // Пересылка в групповой чат
                    else if (target_type == "group") {
                        if (!db.groupExists(target_name) || !db.isGroupMember(target_name, username)) {
                            send_to_client(client_fd, "\n[Ошибка: Группа не существует или вы в ней не состоите]\n");
                            continue;
                        }
                        
                        db.saveGroupMessageAdvanced(target_name, username, orig_text, 0, orig_author);
                        send_to_client(client_fd, "\n[Сообщение успешно переслано в группу " + target_name + "]\n");
                    } else {
                        send_to_client(client_fd, "\n[Ошибка: target_type должен быть 'chat' или 'group']\n");
                    }
                }
                // Команда очистки всей истории (личный чат)
                else if (msg == "/clear") {
                    if (db.clearHistoryAdvanced(username, current_chat_with, false)) {
                        send_to_client(client_fd, "__REFRESH_CHAT__");  // Сигнал клиенту очистить экран
                        send_chat_history(client_fd, username, current_chat_with);
                    }
                }
                // Команда удаления одного сообщения (личный чат)
                else if (msg.substr(0, 7) == "/delete") {
                    string args = msg.substr(7);
                    size_t start = args.find_first_not_of(" ");
                    if (start == string::npos) {
                        send_to_client(client_fd, "\n[Использование: /delete <id>]\n");
                        continue;
                    }
                    string id_str = args.substr(start);
                    size_t space = id_str.find(' ');
                    if (space != string::npos) {
                        id_str = id_str.substr(0, space);
                    }
                    
                    try {
                        int msg_id = stoi(id_str);
                        
                        // Проверяем, что сообщение принадлежит этому чату
                        auto all_my_chat_messages = db.getMessages(username, current_chat_with);
                        bool belongs_to_current_chat = false;
                        for (const auto& m_str : all_my_chat_messages) {
                            size_t first_pipe = m_str.find('|');
                            if (first_pipe != string::npos) {
                                if (m_str.substr(0, first_pipe) == to_string(msg_id)) {
                                    belongs_to_current_chat = true;
                                    break;
                                }
                            }
                        }
                        
                        if (!belongs_to_current_chat) {
                            send_to_client(client_fd, "\n[Ошибка: Сообщение с ID " + to_string(msg_id) + " не найдено в текущем диалоге]\n");
                            continue;
                        }
                        
                        // Мягкое удаление: помечаем сообщение как удалённое для этого пользователя
                        if (db.deleteMessageAdvanced(msg_id, username, false)) {
                            send_to_client(client_fd, "__REFRESH_CHAT__");
                            send_chat_history(client_fd, username, current_chat_with);
                        } else {
                            send_to_client(client_fd, "\n[Ошибка: Не удалось удалить сообщение из истории]\n");
                        }
                    } catch (...) {
                        // *** ИСПРАВЛЕНИЕ №3: Добавлен continue для продолжения работы ***
                        send_to_client(client_fd, "\n[Ошибка: Неверный формат ID сообщения]\n");
                        continue;
                    }
                }
                // Команда показа онлайн пользователей
                else if (msg == "/users") {
                    send_to_client(client_fd, "\n=== Пользователи онлайн ===");
                    lock_guard<mutex> lock(clients_mutex);
                    for (auto& [sock, name] : clients) {
                        send_to_client(client_fd, "  - " + name);
                    }
                    send_to_client(client_fd, "===========================\n");
                }
                // Команда выхода из программы
                else if (msg == "/quit") {
                    send_to_client(client_fd, "До свидания!");
                    goto cleanup;
                }
                // Обычное текстовое сообщение (не команда) в личном чате
                else if (!msg.empty() && msg[0] != '/') {
                    string today_date = get_novosibirsk_date();
                    string short_time = get_novosibirsk_time();
                    
                    int new_msg_id = -1;
                    int friend_sock = -1;
                    bool friend_is_chatting_with_me = false;
                    bool i_have_today = false;
                    bool friend_has_today = false;
                    
                    {
                        lock_guard<mutex> lock(clients_mutex);
                        
                        // Проверяем, были ли сегодня сообщения у меня
                        auto my_messages = db.getMessages(username, current_chat_with);
                        for (const auto& m : my_messages) {
                            size_t p_id = m.find('|');
                            if (p_id == string::npos) continue;
                            size_t p1 = m.find('|', p_id + 1);
                            if (p1 == string::npos) continue;
                            
                            string msg_date = m.substr(p_id + 1, p1 - p_id - 1);
                            if (msg_date == today_date) {
                                i_have_today = true;
                                break;
                            }
                        }
                        
                        // Получаем сокет собеседника
                        if (user_to_socket.count(current_chat_with)) {
                            friend_sock = user_to_socket[current_chat_with];
                            if (active_chats.count(current_chat_with) && active_chats[current_chat_with] == username) {
                                friend_is_chatting_with_me = true;
                            }
                        }
                        
                        // Проверяем, были ли сегодня сообщения у собеседника
                        if (friend_sock != -1 && friend_is_chatting_with_me) {
                            auto friend_messages = db.getMessages(current_chat_with, username);
                            for (const auto& m : friend_messages) {
                                size_t p_id = m.find('|');
                                if (p_id == string::npos) continue;
                                size_t p1 = m.find('|', p_id + 1);
                                if (p1 == string::npos) continue;
                                
                                string msg_date = m.substr(p_id + 1, p1 - p_id - 1);
                                if (msg_date == today_date) {
                                    friend_has_today = true;
                                    break;
                                }
                            }
                        }
                        
                        // Сохраняем сообщение в БД
                        new_msg_id = db.saveMessageAdvanced(username, current_chat_with, msg, 0, "");
                    }
                    
                    if (new_msg_id != -1) {
                        string date_plate = "------------------ " + today_date + " ------------------";
                        
                        // Если это первое сообщение за сегодня, показываем разделитель
                        if (!i_have_today) {
                            send_to_client(client_fd, date_plate);
                        }
                        
                        string formatted_msg = "(" + to_string(new_msg_id) + ") [" + short_time + "] " + username + ": " + msg;
                        send_to_client(client_fd, formatted_msg);
                        
                        // Отправляем сообщение собеседнику, если он онлайн
                        if (friend_sock != -1) {
                            if (friend_is_chatting_with_me) {
                                if (!friend_has_today) {
                                    send_to_client(friend_sock, date_plate);
                                }
                                send_to_client(friend_sock, formatted_msg);
                            } else {
                                send_to_client(friend_sock, "\n[Уведомление: Новое личное сообщение от @" + username + "]");
                            }
                        }
                    }
                }
            }
            // =================================================================
            // ЭТАП 3: ПОЛЬЗОВАТЕЛЬ В ГРУППОВОМ ЧАТЕ
            // =================================================================
            else if (!current_group.empty()) {
                // Проверка: не исключили ли пользователя из группы во время чата?
                if (!db.isGroupMember(current_group, username)) {
                    if (msg == "/exit") {
                        send_to_client(client_fd, "\n[Вы вернулись в главное меню]\n");
                        {
                            lock_guard<mutex> lock(clients_mutex);
                            active_groups.erase(username);
                        }
                        current_group.clear();
                    } else {
                        send_to_client(client_fd, "\n[Ошибка: Вы были исключены из этой группы и не можете совершать никаких действий!]\n");
                        send_to_client(client_fd, "[Введите /exit, чтобы вернуться в главное меню]\n");
                    }
                    continue;
                }
                
                // Команда выхода из группы (в главное меню, но остаёмся в группе)
                if (msg == "/exit") {
                    send_to_client(client_fd, "\n[Вы покинули групповой чат " + current_group + "]\n");
                    {
                        lock_guard<mutex> lock(clients_mutex);
                        active_groups.erase(username);
                    }
                    current_group.clear();
                }
                // Команда полного выхода из группы (покидаем группу)
                else if (msg == "/gleave") {
                    db.removeGroupMember(current_group, username);
                    send_to_client(client_fd, "\n[Вы вышли из состава группы " + current_group + "]\n");
                    {
                        lock_guard<mutex> lock(clients_mutex);
                        active_groups.erase(username);
                    }
                    current_group.clear();
                }
                // Команда очистки (скрыть все сообщения для себя в группе)
                else if (msg == "/clear") {
                    auto group_messages = db.getGroupMessages(current_group);
                    // СОХРАНЯЕМ В БД, что пользователь скрыл эти сообщения
                    for (const auto& m_str : group_messages) {
                        size_t first_pipe = m_str.find('|');
                        if (first_pipe != string::npos) {
                            int msg_id = stoi(m_str.substr(0, first_pipe));
                            db.hideGroupMessageForUser(username, msg_id);
                        }
                    }
                    send_to_client(client_fd, "__REFRESH_CHAT__");
                    send_group_history(client_fd, username, current_group);
                }
                // Команда ответа на сообщение в группе
                else if (msg.substr(0, 6) == "/reply") {
                    string args = msg.substr(6);
                    size_t start = args.find_first_not_of(" ");
                    if (start == string::npos) {
                        send_to_client(client_fd, "\n[Использование: /reply <id> <текст>]\n");
                        continue;
                    }
                    args = args.substr(start);
                    size_t space = args.find(' ');
                    if (space == string::npos) {
                        send_to_client(client_fd, "\n[Ошибка: Введите текст ответа]\n");
                        continue;
                    }
                    
                    // *** ИСПРАВЛЕНИЕ №2: Безопасное преобразование ID в число ***
                    // Раньше при вводе букв вместо цифр сервер падал.
                    // Теперь обрабатываем исключение и продолжаем работу.
                    int reply_id = 0;
                    string reply_text = args.substr(space + 1);
                    try {
                        reply_id = stoi(args.substr(0, space));
                    } catch (...) {
                        send_to_client(client_fd, "\n[Ошибка: ID сообщения должен быть числом!]\n");
                        continue;
                    }
                    
                    string orig = db.getMessageText(reply_id, true, username);
                    
                    // ПРОВЕРЯЕМ, НЕ СКРЫТО ЛИ СООБЩЕНИЕ (из БД)
                    bool is_locally_deleted = false;
                    vector<int> my_hidden = db.getHiddenGroupMessages(username);
                    if (find(my_hidden.begin(), my_hidden.end(), reply_id) != my_hidden.end()) {
                        is_locally_deleted = true;
                    }
                    
                    if (orig.empty() || is_locally_deleted) {
                        send_to_client(client_fd, "\n[Ошибка: Вы не можете ответить на это сообщение, так как оно удалено]\n");
                        continue;
                    }
                    
                    int new_msg_id = db.saveGroupMessageAdvanced(current_group, username, reply_text, reply_id, "");
                    
                    string short_time = get_novosibirsk_time();
                    string formatted_msg = "(" + to_string(new_msg_id) + ") [" + short_time + "] [Ответ на (" + to_string(reply_id) + ") " + orig + "] " + username + ": " + reply_text;
                    
                    send_to_client(client_fd, formatted_msg);
                    
                    // Рассылаем ответ всем участникам группы, находящимся в чате
                    auto members = db.getGroupMembers(current_group);
                    lock_guard<mutex> lock(clients_mutex);
                    for (const auto& member : members) {
                        if (member != username && user_to_socket.count(member)) {
                            int member_sock = user_to_socket[member];
                            if (active_groups.count(member) && active_groups[member] == current_group) {
                                send_to_client(member_sock, formatted_msg);
                            } else {
                                send_to_client(member_sock, "\n[Уведомление: В группе [" + current_group + "] от @" + username + " ответ на сообщение]");
                            }
                        }
                    }
                }
                // Команда пересылки сообщения из группы
                else if (msg.substr(0, 4) == "/fwd") {
                    string args = msg.substr(4);
                    stringstream ss(args);
                    int msg_id;
                    string target_type, target_name;
                    if (!(ss >> msg_id >> target_type >> target_name)) {
                        send_to_client(client_fd, "\n[Использование: /fwd <id> chat <ник> или /fwd <id> group <название>]\n");
                        continue;
                    }
                    
                    string orig_info = db.getMessageText(msg_id, true, username);
                    
                    // ПРОВЕРЯЕМ, НЕ СКРЫТО ЛИ СООБЩЕНИЕ (из БД)
                    bool is_locally_deleted = false;
                    vector<int> my_hidden = db.getHiddenGroupMessages(username);
                    if (find(my_hidden.begin(), my_hidden.end(), msg_id) != my_hidden.end()) {
                        is_locally_deleted = true;
                    }
                    
                    // Проверяем, что сообщение из этой группы
                    if (!orig_info.empty() && !is_locally_deleted) {
                        auto group_msgs = db.getGroupMessages(current_group);
                        bool found = false;
                        for (const auto& gm : group_msgs) {
                            size_t p_id = gm.find('|');
                            if (p_id != string::npos && gm.substr(0, p_id) == to_string(msg_id)) {
                                found = true;
                                break;
                            }
                        }
                        if (!found) orig_info = "";
                    }
                    
                    if (orig_info.empty() || is_locally_deleted) {
                        send_to_client(client_fd, "\n[Ошибка: Сообщение с ID " + to_string(msg_id) + " не найдено в этой группе или ваших чатах]\n");
                        continue;
                    }
                    
                    size_t colon = orig_info.find(": ");
                    string orig_author = orig_info.substr(0, colon);
                    string orig_text = orig_info.substr(colon + 2);
                    
                    // Пересылка в личный чат
                    if (target_type == "chat") {
                        if (!db.userExists(target_name)) {
                            send_to_client(client_fd, "\n[Ошибка: Пользователь " + target_name + " не существует]\n");
                            continue;
                        }
                        
                        int new_fwd_id = db.saveMessageAdvanced(username, target_name, orig_text, 0, orig_author);
                        send_to_client(client_fd, "\n[Сообщение успешно переслано в чат к " + target_name + "]\n");
                        
                        string short_time = get_novosibirsk_time();
                        string formatted_fwd = "(" + to_string(new_fwd_id) + ") [" + short_time + "] [Переслано от " + orig_author + "] " + username + ": " + orig_text;
                        
                        if (target_name == current_chat_with) {
                            send_to_client(client_fd, formatted_fwd);
                        }
                        
                        int target_sock = -1;
                        bool target_is_chatting_with_me = false;
                        {
                            lock_guard<mutex> lock(clients_mutex);
                            if (user_to_socket.count(target_name)) {
                                target_sock = user_to_socket[target_name];
                                if (active_chats.count(target_name) && active_chats[target_name] == username) {
                                    target_is_chatting_with_me = true;
                                }
                            }
                        }
                        if (target_sock != -1) {
                            if (target_is_chatting_with_me) {
                                send_to_client(target_sock, formatted_fwd);
                            } else {
                                send_to_client(target_sock, "\n[Уведомление: @" + username + " переслал сообщение вам]");
                            }
                        }
                    }
                    // Пересылка в другую группу
                    else if (target_type == "group") {
                        if (!db.groupExists(target_name) || !db.isGroupMember(target_name, username)) {
                            send_to_client(client_fd, "\n[Ошибка: Группа не существует или вы в ней не состоите]\n");
                            continue;
                        }
                        
                        int new_fwd_id = db.saveGroupMessageAdvanced(target_name, username, orig_text, 0, orig_author);
                        send_to_client(client_fd, "\n[Сообщение успешно переслано в группу " + target_name + "]\n");
                        
                        string short_time = get_novosibirsk_time();
                        string formatted_fwd = "(" + to_string(new_fwd_id) + ") [" + short_time + "] [Переслано от " + orig_author + "] " + username + ": " + orig_text;
                        
                        if (target_name == current_group) {
                            send_to_client(client_fd, formatted_fwd);
                        }
                        
                        // Рассылаем всем участникам целевой группы, находящимся в чате
                        auto members = db.getGroupMembers(target_name);
                        lock_guard<mutex> lock(clients_mutex);
                        for (const auto& member : members) {
                            if (member != username && user_to_socket.count(member)) {
                                int member_sock = user_to_socket[member];
                                if (active_groups.count(member) && active_groups[member] == target_name) {
                                    send_to_client(member_sock, formatted_fwd);
                                } else {
                                    send_to_client(member_sock, "\n[Уведомление: @" + username + " переслал сообщение в группу [" + target_name + "]]");
                                }
                            }
                        }
                    } else {
                        send_to_client(client_fd, "\n[Ошибка: target_type должен быть 'chat' или 'group']\n");
                    }
                }
                // Команда показа онлайн пользователей
                else if (msg == "/users") {
                    send_to_client(client_fd, "\n=== Пользователи онлайн ===");
                    lock_guard<mutex> lock(clients_mutex);
                    for (auto& [sock, name] : clients) {
                        send_to_client(client_fd, "  - " + name);
                    }
                    send_to_client(client_fd, "===========================\n");
                }
                // Команда выхода из программы
                else if (msg == "/quit") {
                    send_to_client(client_fd, "До свидания!");
                    goto cleanup;
                }
                // Команда удаления (скрытия) сообщения в группе
                else if (msg.substr(0, 7) == "/delete") {
                    string args = msg.substr(7);
                    size_t start = args.find_first_not_of(" ");
                    if (start == string::npos) {
                        send_to_client(client_fd, "\n[Использование: /delete <id>]\n");
                        continue;
                    }
                    string id_str = args.substr(start);
                    size_t space = id_str.find(' ');
                    if (space != string::npos) {
                        id_str = id_str.substr(0, space);
                    }
                    
                    try {
                        int msg_id = stoi(id_str);
                        
                        auto group_messages = db.getGroupMessages(current_group);
                        bool belongs_to_current_group = false;
                        for (const auto& m_str : group_messages) {
                            size_t first_pipe = m_str.find('|');
                            if (first_pipe != string::npos) {
                                if (m_str.substr(0, first_pipe) == to_string(msg_id)) {
                                    belongs_to_current_group = true;
                                    break;
                                }
                            }
                        }
                        
                        if (!belongs_to_current_group) {
                            send_to_client(client_fd, "\n[Ошибка: Сообщение с ID " + to_string(msg_id) + " не найдено в этой группе]\n");
                            continue;
                        }
                        
                        // СОХРАНЯЕМ В БД, что пользователь скрыл это сообщение
                        db.hideGroupMessageForUser(username, msg_id);
                        send_to_client(client_fd, "__REFRESH_CHAT__");
                        send_group_history(client_fd, username, current_group);
                        
                    } catch (...) {
                        // *** ИСПРАВЛЕНИЕ №4: Добавлен continue для продолжения работы ***
                        send_to_client(client_fd, "\n[Ошибка: Неверный формат ID сообщения]\n");
                        continue;
                    }
                }
                // Неизвестная команда
                else if (msg[0] == '/') {
                    send_to_client(client_fd, "\n[Ошибка: Неизвестная команда. Доступны: /reply, /fwd, /delete, /clear, /gleave, /exit]\n");
                }
                // Обычное текстовое сообщение в группе
                else if (!msg.empty() && msg[0] != '/') {
                    string today_date = get_novosibirsk_date();
                    string short_time = get_novosibirsk_time();
                    
                    auto group_messages = db.getGroupMessages(current_group);
                    
                    // ПОЛУЧАЕМ СКРЫТЫЕ СООБЩЕНИЯ ИЗ БД
                    vector<int> my_hidden = db.getHiddenGroupMessages(username);
                    
                    bool sender_has_today = false;
                    for (const auto& m : group_messages) {
                        size_t p_id = m.find('|');
                        if (p_id == string::npos) continue;
                        
                        int msg_id = stoi(m.substr(0, p_id));
                        if (find(my_hidden.begin(), my_hidden.end(), msg_id) != my_hidden.end()) {
                            continue;
                        }
                        
                        size_t p1 = m.find('|', p_id + 1);
                        if (p1 == string::npos) continue;
                        
                        string msg_date = m.substr(p_id + 1, p1 - p_id - 1);
                        if (msg_date == today_date) {
                            sender_has_today = true;
                            break;
                        }
                    }
                    
                    int new_msg_id = db.saveGroupMessageAdvanced(current_group, username, msg, 0, "");
                    
                    string date_plate = "------------------ " + today_date + " ------------------";
                    
                    if (!sender_has_today) {
                        send_to_client(client_fd, date_plate);
                    }
                    
                    string formatted_msg = "(" + to_string(new_msg_id) + ") [" + short_time + "] " + username + ": " + msg;
                    send_to_client(client_fd, formatted_msg);
                    
                    // Рассылаем сообщение всем участникам группы
                    auto members = db.getGroupMembers(current_group);
                    lock_guard<mutex> lock(clients_mutex);
                    for (const auto& member : members) {
                        if (member != username && user_to_socket.count(member)) {
                            int member_sock = user_to_socket[member];
                            if (active_groups.count(member) && active_groups[member] == current_group) {
                                // ПОЛУЧАЕМ СКРЫТЫЕ СООБЩЕНИЯ ПОЛУЧАТЕЛЯ ИЗ БД
                                vector<int> member_hidden = db.getHiddenGroupMessages(member);
                                bool member_has_today = false;
                                for (const auto& m : group_messages) {
                                    size_t p_id = m.find('|');
                                    if (p_id == string::npos) continue;
                                    int m_id = stoi(m.substr(0, p_id));
                                    if (find(member_hidden.begin(), member_hidden.end(), m_id) != member_hidden.end()) continue;
                                    size_t p1 = m.find('|', p_id + 1);
                                    if (p1 == string::npos) continue;
                                    string msg_date = m.substr(p_id + 1, p1 - p_id - 1);
                                    if (msg_date == today_date) {
                                        member_has_today = true;
                                        break;
                                    }
                                }
                                if (!member_has_today) {
                                    send_to_client(member_sock, date_plate);
                                }
                                send_to_client(member_sock, formatted_msg);
                            } else {
                                send_to_client(member_sock, "\n[Уведомление: Новое сообщение в группе [" + current_group + "] от @" + username + "]");
                            }
                        }
                    }
                }
            }
            // =================================================================
            // ЭТАП 4: ГЛАВНОЕ МЕНЮ
            // =================================================================
            else {
                // Команда начала личного чата
                if (msg.substr(0, 6) == "/chat ") {
                    string target = msg.substr(6);
                    size_t start = target.find_first_not_of(" ");
                    if (start != string::npos) {
                        target = target.substr(start);
                    }
                    
                    if (target == username) {
                        send_to_client(client_fd, "\n[Вы не можете чатить с самим собой!]\n");
                    }
                    else if (db.userExists(target)) {
                        current_chat_with = target;
                        {
                            lock_guard<mutex> lock(clients_mutex);
                            active_chats[username] = target;
                        }
                        send_chat_history(client_fd, username, target);
                    } else {
                        send_to_client(client_fd, "\n[Пользователь '" + target + "' не существует]\n");
                    }
                }
                // Команда создания группы
                else if (msg.substr(0, 8) == "/gcreate") {
                    string gname = msg.substr(8);
                    size_t start = gname.find_first_not_of(" ");
                    if (start == string::npos) {
                        send_to_client(client_fd, "\n[Ошибка: /gcreate <имя_группы>]\n");
                        continue;
                    }
                    gname = gname.substr(start);
                    
                    if (gname.find(' ') != string::npos) {
                        send_to_client(client_fd, "\n[Ошибка: Название без пробелов]\n");
                        continue;
                    }
                    
                    if (db.groupExists(gname)) {
                        send_to_client(client_fd, "\n[Ошибка: Группа '" + gname + "' уже существует]\n");
                    } else {
                        if (db.createGroup(gname, username)) {
                            send_to_client(client_fd, "\n[Группа '" + gname + "' создана! Вы создатель]\n");
                        } else {
                            send_to_client(client_fd, "\n[Ошибка при создании группы]\n");
                        }
                    }
                }
                // Команда добавления пользователя в группу
                else if (msg.substr(0, 5) == "/gadd") {
                    string args = msg.substr(5);
                    size_t start = args.find_first_not_of(" ");
                    if (start == string::npos) {
                        send_to_client(client_fd, "\n[Ошибка: /gadd <группа> <ник>]\n");
                        continue;
                    }
                    args = args.substr(start);
                    size_t space = args.find(' ');
                    if (space == string::npos) {
                        send_to_client(client_fd, "\n[Ошибка: Укажите никнейм]\n");
                        continue;
                    }
                    string gname = args.substr(0, space);
                    string target_user = args.substr(space + 1);
                    target_user.erase(0, target_user.find_first_not_of(" "));
                    
                    if (!db.groupExists(gname)) {
                        send_to_client(client_fd, "\n[Ошибка: Группы '" + gname + "' не существует]\n");
                    } else if (!db.isGroupOwner(gname, username)) {
                        send_to_client(client_fd, "\n[Ошибка: Только создатель может добавлять]\n");
                    } else if (!db.userExists(target_user)) {
                        send_to_client(client_fd, "\n[Ошибка: Пользователь '" + target_user + "' не найден]\n");
                    } else {
                        if (db.addGroupMember(gname, target_user)) {
                            send_to_client(client_fd, "\n[" + target_user + " добавлен в " + gname + "]\n");
                            int t_sock = -1;
                            {
                                lock_guard<mutex> lock(clients_mutex);
                                if (user_to_socket.count(target_user)) t_sock = user_to_socket[target_user];
                            }
                            if (t_sock != -1) {
                                send_to_client(t_sock, "\n[Уведомление: Вас добавили в группу '" + gname + "']\n");
                            }
                        }
                    }
                }
                // Команда исключения пользователя из группы
                else if (msg.substr(0, 6) == "/gkick") {
                    string args = msg.substr(6);
                    size_t start = args.find_first_not_of(" ");
                    if (start == string::npos) {
                        send_to_client(client_fd, "\n[Ошибка: /gkick <группа> <ник>]\n");
                        continue;
                    }
                    args = args.substr(start);
                    size_t space = args.find(' ');
                    if (space == string::npos) {
                        send_to_client(client_fd, "\n[Ошибка: Укажите никнейм]\n");
                        continue;
                    }
                    string gname = args.substr(0, space);
                    string target_user = args.substr(space + 1);
                    target_user.erase(0, target_user.find_first_not_of(" "));
                    
                    if (!db.groupExists(gname)) {
                        send_to_client(client_fd, "\n[Ошибка: Группы '" + gname + "' не существует]\n");
                    } else if (!db.isGroupOwner(gname, username)) {
                        send_to_client(client_fd, "\n[Ошибка: Только создатель может исключать]\n");
                    } else if (target_user == username) {
                        send_to_client(client_fd, "\n[Ошибка: Используйте /gleave]\n");
                    } else if (!db.isGroupMember(gname, target_user)) {
                        send_to_client(client_fd, "\n[Ошибка: " + target_user + " не в группе]\n");
                    } else {
                        if (db.removeGroupMember(gname, target_user)) {
                            send_to_client(client_fd, "\n[" + target_user + " исключён из " + gname + "]\n");
                            int t_sock = -1;
                            bool target_was_in_this_exact_group = false;
                            
                            {
                                lock_guard<mutex> lock(clients_mutex);
                                if (user_to_socket.count(target_user)) t_sock = user_to_socket[target_user];
                                
                                if (active_groups.count(target_user) && active_groups[target_user] == gname) {
                                    active_groups.erase(target_user);
                                    target_was_in_this_exact_group = true;
                                }
                            }
                            
                            if (t_sock != -1) {
                                send_to_client(t_sock, "\n[Вас исключили из группы '" + gname + "']\n");
                                
                                if (target_was_in_this_exact_group) {
                                    send_to_client(t_sock, "__REFRESH_CHAT__");
                                    send_to_client(t_sock, "[Введите /exit для возврата]\n");
                                }
                            }
                        }
                    }
                }
                // Команда выхода из группы (по своему желанию)
                else if (msg.substr(0, 7) == "/gleave") {
                    string gname = msg.substr(7);
                    size_t start = gname.find_first_not_of(" ");
                    if (start == string::npos) {
                        send_to_client(client_fd, "\n[Ошибка: /gleave <группа>]\n");
                        continue;
                    }
                    gname = gname.substr(start);
                    
                    if (!db.groupExists(gname)) {
                        send_to_client(client_fd, "\n[Ошибка: Группы '" + gname + "' не существует]\n");
                    } else if (!db.isGroupMember(gname, username)) {
                        send_to_client(client_fd, "\n[Ошибка: Вы не в группе '" + gname + "']\n");
                    } else {
                        if (db.removeGroupMember(gname, username)) {
                            send_to_client(client_fd, "\n[Вы вышли из группы '" + gname + "']\n");
                        }
                    }
                }
                // Команда показа моих групп
                else if (msg == "/mygroups") {
                    auto groups = db.getUserGroups(username);
                    if (groups.empty()) {
                        send_to_client(client_fd, "\n[Вы не состоите ни в одной группе]\n");
                        continue;
                    }
                    
                    string res = "\n====================================\n";
                    res += "=== ВАШИ ГРУППЫ ===\n";
                    res += "====================================\n";
                    
                    for (const auto& gname : groups) {
                        res += "\nГруппа: " + gname;
                        if (db.isGroupOwner(gname, username)) res += " (создатель)";
                        res += "\nУчастники:\n";
                        
                        auto members = db.getGroupMembers(gname);
                        for (const auto& m : members) {
                            res += "  - " + m;
                            if (db.isGroupOwner(gname, m)) res += " (создатель)";
                            res += "\n";
                        }
                    }
                    res += "====================================\n";
                    send_to_client(client_fd, res);
                }
                // Команда показа участников группы
                else if (msg.substr(0, 9) == "/gmembers") {
                    string gname = msg.substr(9);
                    size_t start = gname.find_first_not_of(" ");
                    if (start == string::npos) {
                        send_to_client(client_fd, "\n[Ошибка: /gmembers <группа>]\n");
                        continue;
                    }
                    gname = gname.substr(start);
                    
                    if (!db.groupExists(gname)) {
                        send_to_client(client_fd, "\n[Ошибка: Группы '" + gname + "' не существует]\n");
                    } else if (!db.isGroupMember(gname, username)) {
                        send_to_client(client_fd, "\n[Ошибка: Вы не в этой группе]\n");
                    } else {
                        auto members = db.getGroupMembers(gname);
                        string res = "\n=== Участники группы " + gname + " ===\n";
                        for (const auto& m : members) {
                            res += " - " + m;
                            if (db.isGroupOwner(gname, m)) res += " (создатель)";
                            res += "\n";
                        }
                        res += "====================================\n";
                        send_to_client(client_fd, res);
                    }
                }
                // Команда входа в групповой чат
                else if (msg.substr(0, 6) == "/gjoin") {
                    string gname = msg.substr(6);
                    size_t start = gname.find_first_not_of(" ");
                    if (start == string::npos) {
                        send_to_client(client_fd, "\n[Ошибка: /gjoin <группа>]\n");
                        continue;
                    }
                    gname = gname.substr(start);
                    
                    if (!db.groupExists(gname)) {
                        send_to_client(client_fd, "\n[Ошибка: Группы '" + gname + "' не существует]\n");
                    } else if (!db.isGroupMember(gname, username)) {
                        send_to_client(client_fd, "\n[Ошибка: Вы не в группе '" + gname + "']\n");
                    } else {
                        current_group = gname;
                        {
                            lock_guard<mutex> lock(clients_mutex);
                            active_groups[username] = gname;
                        }
                        send_group_history(client_fd, username, gname);
                    }
                }
                // Команда показа онлайн пользователей
                else if (msg == "/users") {
                    send_to_client(client_fd, "\n=== Пользователи онлайн ===");
                    lock_guard<mutex> lock(clients_mutex);
                    for (auto& [sock, name] : clients) {
                        send_to_client(client_fd, "  - " + name);
                    }
                    send_to_client(client_fd, "===========================\n");
                }
                // Команда выхода из программы
                else if (msg == "/quit") {
                    send_to_client(client_fd, "До свидания!");
                    goto cleanup;
                }
                // Если пользователь ввёл что-то, не находясь в чате
                else if (!msg.empty()) {
                    send_to_client(client_fd, "\n[Вы не в чате. Используйте /chat <ник> или /gjoin <группа>]\n");
                }
            }
        }
    }
    
cleanup:
    {
        lock_guard<mutex> lock(clients_mutex);
        active_chats.erase(username);
        active_groups.erase(username);
    }
    close(client_fd);
}

// ============================================================================
// ТОЧКА ВХОДА (main)
// ============================================================================

/**
 * Главная функция сервера
 * 
 * Что делает:
 * 1. Инициализирует базу данных SQLite (создаёт таблицы)
 * 2. Создаёт TCP-сокет
 * 3. Привязывается к порту 8080 с опцией SO_REUSEADDR
 * 4. Начинает слушать входящие соединения
 * 5. Для каждого клиента создаёт отдельный поток (handle_client)
 * 
 * Запуск: ./server
 * Порт: 8080 (жёстко задан)
 * 
 * Особенности:
 * - SO_REUSEADDR позволяет перезапустить сервер без ожидания освобождения порта
 * - INADDR_ANY означает "слушать все сетевые интерфейсы"
 * - Размер очереди ожидания соединений = 10
 */
int main() {
    // Инициализация базы данных (создаёт таблицы, включая deleted_group_messages)
    if (!db.init()) {
        cerr << "Ошибка инициализации БД" << endl;
        return 1;
    }
    
    // Создание сокета
    // AF_INET - IPv4, SOCK_STREAM - TCP
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        cerr << "Ошибка сокета" << endl;
        return 1;
    }
    
    // Установка опции SO_REUSEADDR
    // Позволяет переиспользовать порт сразу после остановки сервера
    // Без этого пришлось бы ждать 2-3 минуты, пока порт освободится
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    // Настройка адреса сервера
    struct sockaddr_in address;
    address.sin_family = AF_INET;           // IPv4
    address.sin_addr.s_addr = INADDR_ANY;   // Слушаем все сетевые интерфейсы
    address.sin_port = htons(8080);         // Порт 8080 (htons - перевод в сетевой порядок байт)
    
    // Привязка сокета к адресу и порту
    if (::bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        cerr << "Ошибка привязки порта" << endl;
        return 1;
    }
    
    // Начинаем слушать входящие соединения
    // Второй аргумент (10) - размер очереди ожидающих соединений
    if (listen(server_fd, 10) < 0) {
        cerr << "Ошибка прослушивания" << endl;
        return 1;
    }
    
    log("Сервер запущен на порту 8080");
    
    // Основной цикл сервера
    while (true) {
        // Принимаем новое соединение (блокирующий вызов)
        int client_fd = accept(server_fd, nullptr, nullptr);
        if (client_fd < 0) {
            continue;  // Ошибка accept, пропускаем и продолжаем
        }
        
        log("Новый клиент подключился");
        
        // Создаём отдельный поток для клиента
        // thread(handle_client, client_fd) - создаём поток, который выполнит функцию handle_client
        // .detach() - отвязываем поток (он будет работать независимо, не нужно join)
        thread(handle_client, client_fd).detach();
    }
    
    // Эта часть никогда не достижима (бесконечный цикл)
    // Но на всякий случай закрываем сокет
    close(server_fd);
    return 0;
}
