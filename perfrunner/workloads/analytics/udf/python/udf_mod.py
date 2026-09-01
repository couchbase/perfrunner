"""Python UDFs for the analytics UDF tests: cheap by design, so the sandbox boundary dominates."""


class Metrics:
    def net_pay(self, bal, ytd, disc):
        return {"netPay": (bal + ytd) - disc}

    def net_pay_scalar(self, bal, ytd, disc):
        return (bal + ytd) - disc

    def eq(self, a, b):
        return a == b

    def score(self, x, spin):
        # Deliberately linear in `spin`, so the UDF's own work dominates; do not optimise it
        acc = 1
        for i in range(spin):
            acc = (acc * 1103515245 + i + 12345) & 0x7FFFFFFF
        return acc ^ (x if isinstance(x, int) else 0)
