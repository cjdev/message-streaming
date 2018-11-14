
@Library('gitlab.cj.com/ad-systems/jenkins-utils@master') _

node("!outoforder") {
  ansiColor('xterm') {
    checkout scm
    gitCommit = sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim()
    currentBuild.displayName = gitCommit

    withMaven(
      maven: 'M3',
      jdk: 'JDK 8',
      mavenLocalRepo: '.repository',
      mavenSettingsConfig: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1394489441612'
    ) {

      stage('Maven Build') {
        withCredentials([[$class: 'AmazonWebServicesCredentialsBinding', accessKeyVariable: 'AWS_ACCESS_KEY_ID', credentialsId: 'agitators-aws-test-resources', secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
          sh "mvn -U clean install -Pall-tests"
        }
      }

      stage('Maven Deploy') {
        try {
          sh "mvn deploy -DskipTests"
        } catch (err) {
          echo "$err"
          currentBuild.result = "UNSTABLE"
        }
      }

    }
  }
}
