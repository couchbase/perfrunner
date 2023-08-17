#include <stdio.h>
#include <libcouchbase/couchbase.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <getopt.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>
#include <math.h>

struct _args
{
    // Cluster
    const char *host;
    const char *bucket;
    const char *username;
    const char *password;

    // Doc
    const char *scope;
    const char *coll;

    // Program
    char *keys;
    uint32_t timeout; // microseconds
    uint32_t config_poll_interval; // microseconds
    int threads;
    long num_items;
    uint running_time; // seconds
} args;

pthread_mutex_t tokenMutex;
char *key;
char *get_next_token()
{
    pthread_mutex_lock(&tokenMutex);
    char *new_key;
    if (key != NULL)
    {
        new_key = key;
        key = strtok(NULL, ",");
    }
    else
    {
        key = strtok(args.keys, ",");
        new_key = key;
    }
    pthread_mutex_unlock(&tokenMutex);

    return new_key;
}

typedef struct Doc
{
    char *id;
    uint64_t cas;
    const char *value;
} Doc;

uint stop = 0;
uint p_time = 300;

char *create_doc()
{
    // TODO: randomise
    char *value = "{\"Field_1\": \"some_string\", \"Field_2\": \"some_string\",\"Field_3\": \"some_string\",\"Field_4\": \"some_string\",\"Field_5\": \"some_string\"}";
    return value;
}
static void
print_log(const char *msg, const char *err)
{
    char buff[50];
    struct timeval tv;
    gettimeofday(&tv, NULL);
    int millisec = lrint(tv.tv_usec / 1000.0);
    int secs = millisec / 1000;
    millisec -= (1000 * secs);
    tv.tv_sec = tv.tv_sec + secs;

    strftime(buff, 50, "%Y-%m-%dT%H:%M:%S", localtime(&tv.tv_sec));
    fprintf(stdout, "%s.%03d, %s: %s\n", buff, millisec, msg, err);
}

// LBC callbacks and operations
static void
store_callback(lcb_INSTANCE *instance, int cbtype, const lcb_RESPSTORE *resp)
{
    lcb_STATUS rc = lcb_respstore_status(resp);
    if (rc != LCB_SUCCESS)
    {
        if (rc != LCB_ERR_CAS_MISMATCH) // Hack, ignore cas mismatch
            print_log("[FAILURE] Store", lcb_strerror_short(rc));
    }
    else
    {
        print_log("[SUCCESS] Store", lcb_strerror_short(rc));
    }
    (void)cbtype; /* unused argument */
}

static void get_callback(lcb_INSTANCE *instance, int cbtype, const lcb_RESPGET *resp)
{
    Doc *doc;
    lcb_respget_cookie(resp, &doc);
    lcb_STATUS rc = lcb_respget_status(resp);

    if (rc != LCB_SUCCESS)
    {
        print_log("[FAILURE] Read", lcb_strerror_short(rc));
    }
    else
    {
        lcb_respget_cas(resp, &doc->cas);
        doc->value = create_doc();
    }
}

static void diag_callback(lcb_INSTANCE *instance, int cbtype, const lcb_RESPBASE *rb)
{
    const lcb_RESPDIAG *resp = (const lcb_RESPDIAG *)rb;
    char *json;
    size_t json_len;
    lcb_STATUS rc = lcb_respdiag_status(resp);
    if (rc != LCB_SUCCESS)
    {
        fprintf(stderr, "diag failed: %s\n", lcb_strerror_short(rc));
    }
    else
    {
        lcb_respdiag_value(resp, &json, &json_len);
        if (json)
        {
            fprintf(stderr, "%.*s\n", json_len, json);
        }
    }
}

// General benchmark operations

// Bootstrapping
pthread_mutex_t instanceMutex;
static lcb_INSTANCE *create_instance()
{
    lcb_STATUS err;
    pthread_mutex_lock(&instanceMutex);
    lcb_INSTANCE *instance;
    lcb_CREATEOPTS *create_options;
    lcb_createopts_create(&create_options, LCB_TYPE_BUCKET);
    lcb_createopts_bucket(create_options, args.bucket, strlen(args.bucket));
    lcb_createopts_connstr(create_options, args.host, strlen(args.host));
    lcb_createopts_credentials(create_options, args.username, strlen(args.username), args.password, strlen(args.password));

    err = lcb_create(&instance, create_options);
    lcb_createopts_destroy(create_options);
    pthread_mutex_unlock(&instanceMutex);
    if (err != LCB_SUCCESS)
    {
        fprintf(stderr, "Failed to create libcouchbase instance: %s\n", lcb_strerror_short(err));
        exit(EXIT_FAILURE);
    }
    if ((err = lcb_connect(instance)) != LCB_SUCCESS)
    {
        fprintf(stderr, "Failed to initiate connect: %s\n", lcb_strerror_short(err));
        lcb_destroy(instance);
        exit(EXIT_FAILURE);
    }
    lcb_wait(instance, LCB_WAIT_NOCHECK);
    if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS)
    {
        fprintf(stderr, "Couldn't establish connection to cluster: %s\n", lcb_strerror_short(err));
        lcb_destroy(instance);
        exit(EXIT_FAILURE);
    }

    lcb_cntl(instance, LCB_CNTL_SET, LCB_CNTL_OP_TIMEOUT, &args.timeout);
    lcb_cntl(instance, LCB_CNTL_SET, LCB_CNTL_CONFIG_POLL_INTERVAL, &args.config_poll_interval);
    lcb_install_callback(instance, LCB_CALLBACK_GET, (lcb_RESPCALLBACK)get_callback);
    lcb_install_callback(instance, LCB_CALLBACK_STORE, (lcb_RESPCALLBACK)store_callback);
    lcb_install_callback(instance, LCB_CALLBACK_DIAG, diag_callback);

    return instance;
}

void *program(void *args)
{
    sleep(p_time);
    stop = 1;
    return 0;
}

#define program_time_controller \
    pthread_t timer_thread;     \
    pthread_create(&timer_thread, NULL, program, NULL);

// Printing and finalising
#define print_results \
    fprintf(stderr, "Run completed\n")

void *benchmark(void *vargp);

// Arguments
void read_arguments(int argc, char **argv)
{
    static struct option long_options[] = {
        {"host", required_argument, NULL, 'h'},
        {"bucket", required_argument, NULL, 'b'},
        {"username", required_argument, NULL, 'u'},
        {"password", required_argument, NULL, 'w'},
        {"num-items", required_argument, NULL, 'i'},
        {"num-threads", required_argument, NULL, 't'},
        {"timeout", required_argument, NULL, 'd'},
        {"keys", required_argument, NULL, 'k'},
        {"time", required_argument, NULL, 'r'},
        {"c-poll-interval", required_argument, NULL, 'c'},
        {NULL, 0, NULL, 0}};
    char c;
    while ((c = getopt_long(argc, argv, "hbuwitd:", long_options, NULL)) != -1)
    {
        int this_option_optind = optind ? optind : 1;
        switch (c)
        {
        case 'h':
            args.host = optarg;
            break;
        case 'b':
            args.bucket = optarg;
            break;
        case 'u':
            args.username = optarg;
            break;
        case 'w':
            args.password = optarg;
            break;
        case 'i':
            args.num_items = atol(optarg);
            break;
        case 't':
            args.threads = atoi(optarg);
            break;
        case 'd':
            args.timeout = atoi(optarg);
            break;
        case 'c':
            args.config_poll_interval = atoi(optarg);
            break;
        case 'k':
            args.keys = optarg;
            break;
        case 'r':
            args.running_time = atoi(optarg);
            p_time = args.running_time;
            break;
        default:
            fprintf(stderr, "[WARNING] Unknown option %c, ignoring value '%s'", c, optarg);
        }
    }
    args.scope = "_default";
    args.coll = "_default";
    fprintf(stderr, "Running benchmark for %d sec on host %s (%s) with [items: %ld, threads: %d, timeout: %dus] \n", args.running_time, args.host, args.bucket, args.num_items, args.threads, args.timeout);
}