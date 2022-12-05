name: 🐞 Bug Report
description: File a new bug report
title: '[Bug]: <title>'
labels: [Bug]
body:
  - type: markdown
    attributes:
      value: ':stop_sign: _For support questions, please visit [GraphAr Discussions](https://github.com/alibaba/GraphAr/discussions) instead._'
  - type: checkboxes
    attributes:
      label: 'Is there an existing issue for this?'
      description: 'Please [search :mag: the issues](https://github.com/alibaba/GraphAr/issues) to check if this bug has already been reported.'
      options:
      - label: 'I have searched the existing issues'
        required: true
  - type: textarea
    attributes:
      label: 'Current Behavior'
      description: 'Describe the problem you are experiencing.  **Please do not paste your logs here.**  Screenshots are welcome.'
    validations:
      required: true
  - type: textarea
    attributes:
      label: 'Expected Behavior'
      description: 'Describe what you expect to happen instead.'
    validations:
      required: true
  - type: textarea
    attributes:
      label: 'Minimal Reproducible Example'
      description: |
        Please provide a the _smallest, complete code snippet_ that GraphAr's maintainers can run to reproduce the issue ([read more about what this entails](https://stackoverflow.com/help/minimal-reproducible-example)).  Failing this, any sort of reproduction steps are better than nothing!

        If the result is more than a screenful of text _or_ requires multiple files, please:

        - _Attach_ (do not paste) it to this textarea, _or_
        - Put it in a [Gist](https://gist.github.com) and paste the link, _or_
        - Provide a link to a new or existing public repository exhibiting the issue
    validations:
      required: true
  - type: textarea
    attributes:
      label: 'Environment'
      description: 'Please provide the following information about your environment; feel free to remove any items which are not relevant.'
      value: |
          - Operating system:
          - GraphAr version:
    validations:
      required: false
  - type: input
    attributes:
      label: 'Link to GraphAr Logs'
      description: |
        Create a [Gist](https://gist.github.com)—which contains your _full_ GraphAr logs—and link it here.  Alternatively, you can attach a logfile to this issue (drag it into the "Further Information" field below).

        :warning: _Remember to redact or remove any sensitive information!_
      placeholder: 'https://gist.github.com/...'
  - type: textarea
    attributes:
      label: Further Information
      description: |
        Links? References? Anything that will give us more context about the issue you are encountering!

        _Tip: You can attach images or log files by clicking this area to highlight it and then dragging files in._
    validations:
      required: false
  - type: markdown
    attributes:
      value: ':stop_sign: _For support questions, please visit [GraphAr Discussions](https://github.com/alibaba/GraphAr/discussions) instead._'
