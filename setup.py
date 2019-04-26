from setuptools import setup

setup(name='lemkpg',
      version='1.3',
      description='lemkpg package give simple API interface for quick access to PostgreSQL DB via async way',
      url='https://github.com/Daniil-Kost/lemkpg',
      author='Daniil Kostyshak',
      author_email='lemk@ukr.net',
      license='MIT',
      packages=['lemkpg'],
      install_requires=[
          'aiopg',
          'psycopg2',
      ],
      classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
      ],
      zip_safe=False)
