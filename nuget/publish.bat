@rem \bin\nuget-2.8.5\NuGet.exe pack ..\src\kafka4net.csproj -Build -Symbols -Prop Configuration=Release 
nuget.exe pack ..\src\kafka4net.csproj -Build -Symbols -Prop Configuration=Release -Prop target=Clean -MSBuildVersion 14 -Verbosity detailed -Version 2.0.0-compression0001

@rem 