try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': ('Lethril - Eavesdrop on what the world '
                    'is saying on twitter'),
    'author': 'Deepu Thomas Philip',
    'url': '',
    'download_url': '',
    'author_email': 'deepu.dtp@gmail.com',
    'version': '0.1.1',
    'install_requires': ['nose'],
    'packages': ['lethril'],
    'scripts': [],
    'name': 'Lethril'
}

setup(**config)