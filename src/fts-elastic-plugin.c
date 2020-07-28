/* Copyright (c) 2006-2012 Dovecot authors, see the included COPYING file */
/* Copyright (c) 2014 Joshua Atkins <josh@ascendantcom.com> */
/* Copyright (c) 2019-2020 Filip Hanes <filip.hanes@gmail.com> */

#include "lib.h"
#include "array.h"
#include "http-client.h"
#include "mailbox-list.h"
#include "mailbox-list-private.h"
#include "mail-storage.h"
#include "mail-storage-private.h"
#include "mail-user.h"
#include "mail-storage-hooks.h"
#include "fts-elastic-plugin.h"
#include "fts-storage.h"
#include <stdlib.h>

#define VIRTUAL_STORAGE_NAME "virtual"

const char *fts_elastic_plugin_version = DOVECOT_ABI_VERSION;
struct http_client *elastic_http_client = NULL;

struct fts_elastic_user_module fts_elastic_user_module =
    MODULE_CONTEXT_INIT(&mail_user_module_register);

static int
fts_elastic_plugin_init_settings(struct mail_user *user,
                                 struct fts_elastic_settings *set,
                                 const char *str)
{
    FUNC_START();
    i_debug("fts_elastic_plugin_init_settings");
    const char *const *tmp;

    /* validate our parameters */
    if (user == NULL || set == NULL)
    {
        i_error("fts_elastic: critical error initialisation");
        return -1;
    }

    if (str == NULL)
    {
        str = "";
    }
    i_debug("\n test =====================");
    set->bulk_size = 5 * 1024 * 1024; /* 5 MB */
    set->refresh_by_fts = TRUE;
    set->refresh_on_update = FALSE;

    tmp = t_strsplit_spaces(str, " ");
    for (; *tmp != NULL; tmp++)
    {
        if (strncmp(*tmp, "url=", 4) == 0)
        {
            set->url = p_strdup(user->pool, *tmp + 4);
        }
        else if (strcmp(*tmp, "debug") == 0)
        {
            set->debug = TRUE;
        }
        else if (strncmp(*tmp, "rawlog_dir=", 11) == 0)
        {
            set->rawlog_dir = p_strdup(user->pool, *tmp + 11);
        }
        else if (strncmp(*tmp, "bulk_size=", 10) == 0)
        {
            if (str_to_uint(*tmp + 10, &set->bulk_size) < 0 || set->bulk_size == 0)
            {
                i_error("fts_elastic: bulk_size='%s' must be a positive integer", *tmp + 10);
                return -1;
            }
        }
        else if (strncmp(*tmp, "refresh=", 8) == 0)
        {
            if (strcmp(*tmp + 8, "never") == 0)
            {
                set->refresh_on_update = FALSE;
                set->refresh_by_fts = FALSE;
            }
            else if (strcmp(*tmp + 8, "update") == 0)
            {
                set->refresh_on_update = TRUE;
            }
            else if (strcmp(*tmp + 8, "fts") == 0)
            {
                set->refresh_by_fts = TRUE;
            }
            else
            {
                i_error("fts_elastic: Invalid setting for refresh: %s", *tmp + 8);
                return -1;
            }
        }
        else
        {
            i_error("fts_elastic: Invalid setting: %s", *tmp);
            return -1;
        }
    }

    i_debug("\n ccccccccccccc =====================");
    FUNC_END();
    return 0;
}

static void fts_elastic_mail_user_create(struct mail_user *user, const char *env)
{
    FUNC_START();
    i_debug("fts_elastic_mail_user_create");
    struct fts_elastic_user *fuser = NULL;

    /* validate our parameters */
    if (user == NULL || env == NULL)
    {
        i_error("fts_elastic: critical error during mail user creation");
    }
    else
    {
        fuser = p_new(user->pool, struct fts_elastic_user, 1);
        if (fts_elastic_plugin_init_settings(user, &fuser->set, env) < 0)
        {
            /* invalid settings, disabling */
            return;
        }

        MODULE_CONTEXT_SET(user, fts_elastic_user_module, fuser);
    }
    FUNC_END();
}

static void fts_elastic_mail_user_created(struct mail_user *user)
{
    FUNC_START();
    const char *env = NULL;

    /* validate our parameters */
    if (user == NULL)
    {
        i_error("fts_elastic: critical error during mail user creation");
    }
    else
    {
        env = mail_user_plugin_getenv(user, "fts_elastic");

        if (env != NULL)
        {
            fts_elastic_mail_user_create(user, env);
        }
    }
    FUNC_END();
}

struct elastic_fts_backend_update_context
{
    struct fts_backend_update_context ctx;

    struct mailbox *prev_box;
    char box_guid[MAILBOX_GUID_HEX_LENGTH + 1];
    const char *username;

    uint32_t uid;

    /* used to build multi-part messages. */
    string_t *current_key;
    buffer_t *current_value;

    ARRAY(struct elastic_fts_field)
    fields;

    /* build a json string for bulk indexing */
    string_t *json_request;

    unsigned int body_open : 1;
    unsigned int documents_added : 1;
    unsigned int expunges : 1;
};

static void
ftss_backend_elastic_update_set_mailbox(struct fts_backend_update_context *_ctx,
                                        struct mailbox *box)
{
    FUNC_START();
    struct elastic_fts_backend_update_context *ctx =
        (struct elastic_fts_backend_update_context *)_ctx;
    FUNC_END();
}

static void fts_elastic_mail_precache(struct mail *_mail)
{
    struct mail_private *mail = (struct mail_private *)_mail;
    struct fts_elastic_mail *fmail = FTS_MAIL_CONTEXT(mail);
    struct fts_elastic_transaction_context *ft = FTS_CONTEXT(_mail->transaction);

    fmail->module_ctx.super.precache(_mail);
    if (fmail->virtual_mail)
    {
        if (ft->highest_virtual_uid < _mail->uid)
            ft->highest_virtual_uid = _mail->uid;
    }
    else
        T_BEGIN
        {
            /* not implemented */
        }
    T_END;
}

static void fts_elastic_mail_allocated(struct mail *_mail)
{
    FUNC_START();
    struct mail_private *mail = (struct mail_private *)_mail;
    struct mail_vfuncs *v = mail->vlast;
    struct fts_elastic_mailbox *fbox = FTS_CONTEXT(_mail->box);
    struct fts_elastic_mail *fmail;

    fmail = p_new(mail->pool, struct fts_elastic_mail, 1);
    fmail->module_ctx.super = *v;
    mail->vlast = &fmail->module_ctx.super;
    fmail->virtual_mail =
        strcmp(_mail->box->storage->name, VIRTUAL_STORAGE_NAME) == 0;
    v->precache = fts_elastic_mail_precache;
    MODULE_CONTEXT_SET(mail, fts_elastic_mail_module, fmail);
    FUNC_END();
}

static void fts_elastic_mailbox_list_deinit(struct mailbox_list *list)
{
    struct fts_elastic_mailbox_list *flist = FTS_LIST_CONTEXT(list);

    fts_backend_deinit(&flist->backend);
    flist->module_ctx.super.deinit(list);
}

static void fts_elastic_mailbox_list_created(struct mailbox_list *list)
{
    struct fts_backend *backend = &fts_backend_elastic;
    const char *name, *path, *error;

    name = mail_user_plugin_getenv(list->ns->user, "fts");
    if (name == NULL)
    {
        if (list->mail_set->mail_debug)
            i_debug("fts_elastic: No fts_elastic setting - plugin disabled");
        return;
    }

    if (!mailbox_list_get_root_path(list, MAILBOX_LIST_PATH_TYPE_INDEX, &path))
    {
        if (list->mail_set->mail_debug)
        {
            i_debug("fts_elastic: Indexes disabled for namespace '%s'",
                    list->ns->prefix);
        }
        return;
    }
    if (fts_backend_init(name, list->ns, &error, &backend) < 0)
    {
        i_error("fts: Failed to initialize backend '%s': %s",
                name, error);
    }
    else
    {
        struct fts_elastic_mailbox_list *flist;
        struct mailbox_list_vfuncs *v = list->vlast;

        if ((backend->flags & FTS_BACKEND_FLAG_FUZZY_SEARCH) != 0)
            list->ns->user->fuzzy_search = TRUE;

        flist = p_new(list->pool, struct fts_elastic_mailbox_list, 1);
        flist->module_ctx.super = *v;
        flist->backend = backend;
        list->vlast = &flist->module_ctx.super;
        v->deinit = fts_elastic_mailbox_list_deinit;
        MODULE_CONTEXT_SET(list, fts_elastic_mailbox_list_module, flist);
    }
}

static struct mail_storage_hooks fts_elastic_mail_storage_hooks = {
    .mailbox_list_created = fts_elastic_mailbox_list_created,
    .mail_user_created = fts_elastic_mail_user_created, // listen hook for new message was created by user
    .mail_allocated = fts_elastic_mail_allocated        // listen hook for new mail
};

void fts_elastic_plugin_init(struct module *module)
{
    FUNC_START();
    fts_backend_register(&fts_backend_elastic);
    mail_storage_hooks_add(module, &fts_elastic_mail_storage_hooks);
    FUNC_END();
}

void fts_elastic_plugin_deinit(void)
{
    FUNC_START();
    fts_backend_unregister(fts_backend_elastic.name);
    mail_storage_hooks_remove(&fts_elastic_mail_storage_hooks);
    if (elastic_http_client != NULL)
        http_client_deinit(&elastic_http_client);

    FUNC_END();
}

const char *fts_elastic_plugin_dependencies[] = {"fts", NULL};
