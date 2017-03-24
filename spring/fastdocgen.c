#include <Python.h>

struct module_state {
    PyObject *error;
};

#define GETSTATE(m) ((struct module_state*)PyModule_GetState(m))

static PyObject *
build_achievements(PyObject *self, PyObject *args)
{
    char *alphabet;
    if (!PyArg_ParseTuple(args, "s", &alphabet))
        return NULL;

    const int offset = 42;
    const int max_len = 16;

    int i;
    int achievement = 256;
    int achievements[max_len];
    int num_valid = 0;

    for (i = 0; i < max_len; i++) {
        int hex = strtol((char[]){alphabet[i+offset], 0}, NULL, 16);
        achievement = (achievement + hex * i) % 512;
        if (achievement < 256) {
            num_valid++;
            achievements[i] = achievement;
        } else {
            achievements[i] = -1;
        }
    }

    PyObject *py_array = PyList_New(num_valid);
    int iter = 0;
    for (i = 0; i < max_len; i++) {
        if (achievements[i] >= 0) {
            PyObject *num = PyLong_FromLong(achievements[i]);
            PyList_SET_ITEM(py_array, iter, num);
            iter++;
        }
    }
    return py_array;
}


static PyMethodDef
fastdocgen_methods[] = {
    {"build_achievements",  build_achievements, METH_VARARGS, NULL},
    {NULL, NULL, 0, NULL}
};

static int fastdocgen_traverse(PyObject *m, visitproc visit, void *arg) {
    Py_VISIT(GETSTATE(m)->error);
    return 0;
}

static int fastdocgen_clear(PyObject *m) {
    Py_CLEAR(GETSTATE(m)->error);
    return 0;
}

static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT,
        "fastdocgen",
        NULL,
        sizeof(struct module_state),
        fastdocgen_methods,
        NULL,
        fastdocgen_traverse,
        fastdocgen_clear,
        NULL
};

PyMODINIT_FUNC
PyInit_fastdocgen(void)
{
    PyObject *m;

	m = PyModule_Create(&moduledef);
    if (m == NULL)
        return NULL;

    return m;
}
