@rem \bin\nuget-2.8.5\NuGet.exe pack ..\src\kafka4net.csproj -Build -Symbols -Prop Configuration=Release 
nuget.exe pack ..\src\kafka4net.csproj -Build -Symbols -Prop Configuration=Release; -MSBuildVersion 14 -Version 2.0.1

@rem -Verbosity detailed