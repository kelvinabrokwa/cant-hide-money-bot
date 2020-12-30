import setuptools

with open('README.md', 'r', encoding='utf-8') as fh:
    long_description = fh.read()

setuptools.setup(
    name='cant-hide-money-bot',
    version='0.0.1',
    author='Kelvin Abrokwa-Johnson',
    author_email='kelvinabrokwa@gmail.com',
    description='A Discord bot for simulated trading',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/kelvinabrokwa/cant-hide-money-bot',
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8',
    install_requires=[
        'autopep8',
        'boto3',
        'click',
        'dataframe-image',
        'discord.py',
        'flake8',
        'flake8-annotations',
        'flake8-import-order',
        'flask',
        'flask-cors',
        'Flask[async]',
        'gunicorn',
        'httpx',
        'imgkit',
        'lxml',
        'matplotlib',
        'mypy',
        'pandas',
        'pyrsistent',
        'pytest',
        'pytest-asyncio',
        'python-dotenv',
        'requests',
        'tabulate',
    ]
)
