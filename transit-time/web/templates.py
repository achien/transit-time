from starlette.templating import Jinja2Templates

_templates = None


def get() -> Jinja2Templates:
    global _templates
    if _templates is None:
        _templates = Jinja2Templates(directory="web/templates")
    return _templates
