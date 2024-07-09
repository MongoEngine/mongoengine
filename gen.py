import sys


def generate(f: str, t: str, args: str = ""):
    return f"""
    @overload
    def __new__(
        cls, {args}
        *,
        required: Literal[False] = ...,
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> {f}[Optional[{t}], Optional[{t}]]: ...
    @overload
    def __new__(
        cls, {args}
        *,
        required: Literal[False] = ...,
        default: Union[{t}, Callable[[], {t}]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> {f}[Optional[{t}], {t}]: ...
    @overload
    def __new__(
        cls, {args}
        *,
        required: Literal[True],
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> {f}[{t}, {t}]: ...
    @overload
    def __new__(
        cls, {args}
        *,
        required: Literal[True],
        default: Union[{t}, Callable[[], {t}]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> {f}[Optional[{t}], {t}]: ...
    """


print(generate(*sys.argv[1:]))
