#include <Python.h>


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
            PyObject *num = PyInt_FromLong(achievements[i]);
            PyList_SET_ITEM(py_array, iter, num);
            iter++;
        }
    }
    return py_array;
}


static PyMethodDef
FastDocGenMethods[] = {
    {"build_achievements",  build_achievements, METH_VARARGS, NULL},
    {NULL, NULL, 0, NULL}
};


PyMODINIT_FUNC
initfastdocgen(void)
{
    PyObject *m;

    m = Py_InitModule("fastdocgen", FastDocGenMethods);
    if (m == NULL)
        return;
}
