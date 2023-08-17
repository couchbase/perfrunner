#include "core/benchmark.h"

void *benchmark(void *vargp)
{
    lcb_STATUS err;
    lcb_INSTANCE *local_instance = create_instance();

    lcb_CMDDIAG *cmd_diag;
    lcb_cmddiag_create(&cmd_diag);
    lcb_diag(local_instance, NULL, cmd_diag);
    while (!stop)
    {
        lcb_CMDGET *cmd_get;
        Doc doc;
        doc.id = get_next_token();
        lcb_cmdget_create(&cmd_get);
        lcb_cmdget_collection(cmd_get, args.scope, strlen(args.scope), args.coll, strlen(args.coll));
        lcb_cmdget_key(cmd_get, doc.id, strlen(doc.id));
        err = lcb_get(local_instance, &doc, cmd_get);
        lcb_cmdget_destroy(cmd_get);
        if (err != LCB_SUCCESS)
        {
            fprintf(stderr, "Failed to schedule get operation: %s\n", lcb_strerror_short(err));
        }
        lcb_wait(local_instance, LCB_WAIT_DEFAULT);
        // fprintf(stderr, "%s %llu\n", doc.id, doc.cas);

        // Update doc
        if (doc.value != NULL)
        {
            lcb_CMDSTORE *cmd_store;
            lcb_cmdstore_create(&cmd_store, LCB_STORE_UPSERT);
            lcb_cmdstore_key(cmd_store, doc.id, strlen(doc.id));
            lcb_cmdstore_value(cmd_store, doc.value, strlen(doc.value));
            lcb_cmdstore_cas(cmd_store, doc.cas);
            err = lcb_store(local_instance, NULL, cmd_store);
            if (err != LCB_SUCCESS)
            {
                fprintf(stderr, "Failed to schedule store operation: %s\n", lcb_strerror_short(err));
            }
            lcb_wait(local_instance, LCB_WAIT_DEFAULT);
        }
    }

    lcb_destroy(local_instance);
    pthread_exit(NULL);
    return 0;
}

int main(int argc, char **argv)
{
    /* Retrieve configuration */
    read_arguments(argc, argv);
    fprintf(stderr, "Starting benchmarking ...\n");
    pthread_mutex_init(&tokenMutex, NULL);
    pthread_mutex_init(&instanceMutex, NULL);

    program_time_controller;
    pthread_t thread_ids[args.threads];
    for (int i = 0; i < args.threads; ++i)
    {
        if (pthread_create(&thread_ids[i], NULL, benchmark, NULL) != 0)
        {
            fprintf(stderr, "Failed creating thread\n");
            exit(EXIT_FAILURE);
        }
    }
    for (int i = 0; i < args.threads; ++i)
    {
        pthread_join(thread_ids[i], NULL);
    }

    pthread_mutex_destroy(&tokenMutex);
    pthread_mutex_destroy(&instanceMutex);
    print_results;

    return 0;
}