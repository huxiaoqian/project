from setuptools import setup

setup(name='classifier',
      version='0.1',
      packages=['classifier'],
      data_files=[('data', ['data/triple_polarity_1.dict', 'data/4groups.csv', 'data/triple_polarity_1.txt', 'data/triple_subjective_1.dict', 'data/triple_subjective_1.txt'])],
      install_requires=[
      ],
      dependency_links=[
      ],
)
