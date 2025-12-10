from pipeliner import Pipe, Pipeline, Split


def add_one(num: int) -> int:
    return num + 1


def add_two(num: int) -> int:
    return num + 2


def add(a: int, b: int) -> int:
    return a + b


def main():
    p = Pipeline(
        (
            Pipe(add_one),
            Pipe(add_one),
            Pipe(add_two),
            Split(
                condition=lambda x: x % 2 == 0,
                if_true=Pipe(add_one),
                if_false=Pipe(add_two),
            ),
            Pipe(add, b=100),
            Split(
                condition=lambda x: x > 1000,
                if_true=Pipe(add, b=9),
                if_false=Pipe(add, 8),
            ),
        )
    )

    # p1 = Pipe(add_one) | Pipe(add_two) | Pipe(add, 10)
    # p2 = Pipe(add, 10) | Split(condition=lambda x: x > 50, if_true=Pipe(add_two), if_false=Pipe(add_one))
    # p = p1 | p2

    result = p.run(6)
    print('Result:', result)


if __name__ == '__main__':
    main()
