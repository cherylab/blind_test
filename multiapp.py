"""Frameworks for running multiple Streamlit applications as a single app.
"""
import streamlit as st

class MultiApp:
    """Framework for combining multiple streamlit applications.
    Usage:
        def foo():
            st.title("Hello Foo")
        def bar():
            st.title("Hello Bar")
        app = MultiApp()
        app.add_app("Foo", foo)
        app.add_app("Bar", bar)
        app.run()
    It is also possible keep each application in a separate file.
        import foo
        import bar
        app = MultiApp()
        app.add_app("Foo", foo.app)
        app.add_app("Bar", bar.app)
        app.run()
    """
    def __init__(self):
        self.apps = []

    def add_app(self, title, func, params):
        """Adds a new application.
        Parameters
        ----------
        func: the python function to render this app.
        title: title of the app. Appears in the dropdown in the sidebar.
        params: any parameters you need to feed into the page function
        """
        self.apps.append({
            "title": title,
            "function": func,
            "parameters": params
        })

    def run(self, logo_path=""):
        if logo_path != "":
            app = col1, col2, col3 = st.sidebar.columns([1,6,1])
            app = col1.write("")
            app = col2.image(logo_path, use_column_width=True)
            app = col3.write("")
            app = st.sidebar.write("<br>", unsafe_allow_html=True)

        app = st.sidebar.radio(
            'Pages',
            self.apps,
            format_func=lambda app: app['title'])

        app['function'](*(app['parameters'][i] for i in range(len(app['parameters']))))